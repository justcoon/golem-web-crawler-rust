use crate::common::get_domain_from_url;
use crate::common_lib::database::DatabaseHelper;
use crate::common_lib::database::PostgresDbConfig;
use crate::encode_params;
use golem_rust::agentic::Config;
use golem_rust::{ConfigSchema, Schema, agent_definition, agent_implementation};
use regex::Regex;
use serde::{Deserialize, Serialize};
use wstd::http::{Body, Client, HeaderValue, Request};

// Configuration for the Fetcher agent, providing DB connection settings.
#[derive(ConfigSchema)]
pub struct FetcherConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
}

// Result of a fetch operation.
#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct FetchResult {
    pub url: String,
    pub title: String,
    pub extracted_links: Vec<String>,
    pub status: u16,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum FetcherError {
    InvalidUrl {
        url: String,
        reason: String,
    },
    RobotsDisallowed {
        url: String,
    },
    HttpFetchFailed {
        url: String,
        status_code: u16,
        message: String,
    },
    PostgresWriteFailed {
        message: String,
    },
}

#[agent_definition(ephemeral)]
pub trait FetcherAgent {
    fn new(#[agent_config] config: Config<FetcherConfig>) -> Self;
    async fn fetch_and_parse(&self, url: String) -> Result<FetchResult, FetcherError>;
}

pub struct FetcherAgentImpl {
    config: Config<FetcherConfig>,
}

#[agent_implementation]
impl FetcherAgent for FetcherAgentImpl {
    fn new(#[agent_config] config: Config<FetcherConfig>) -> Self {
        Self { config }
    }

    async fn fetch_and_parse(&self, url: String) -> Result<FetchResult, FetcherError> {
        // Validate URL format
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(FetcherError::InvalidUrl {
                url: url.clone(),
                reason: "URL must start with http:// or https://".to_string(),
            });
        }

        // Perform HTTP GET and extract body using helper
        let (status, body) = self.fetch_body(&url).await?;

        // Extract title and links using helper
        let (title, extracted_links) = self.extract_content(&url, &body);

        // Persist result to PostgreSQL (ignore errors for now)
        let cfg = self.config.get();
        if let Ok(db_helper) = DatabaseHelper::from(cfg.db) {
            let domain = get_domain_from_url(&url).unwrap_or_default();
            let _ = db_helper.transactional(|tx| {
                let sql = "INSERT INTO page_contents (url, domain, title, http_status, raw_html, extracted_text) \
                           VALUES ($1, $2, $3, $4, $5, $6) \
                           ON CONFLICT (url) DO UPDATE SET \
                           domain = EXCLUDED.domain, \
                           title = EXCLUDED.title, \
                           http_status = EXCLUDED.http_status, \
                           raw_html = EXCLUDED.raw_html, \
                           extracted_text = EXCLUDED.extracted_text, \
                           saved_at = CURRENT_TIMESTAMP";
                tx.execute(sql, encode_params!(&url, &domain, &title, &(status as i32), &body, &body))?;
                Ok(())
            });
        }

        Ok(FetchResult {
            url,
            title,
            extracted_links,
            status,
        })
    }
}

impl FetcherAgentImpl {
    fn resolve_url(&self, base_url: &str, relative: &str) -> Option<String> {
        let base = url::Url::parse(base_url).ok()?;
        base.join(relative).ok().map(|u| u.into())
    }

    fn extract_content(&self, base_url: &str, body: &str) -> (String, Vec<String>) {
        // Extract title using regex
        let title_regex = Regex::new(r"<title>(?P<title>.*?)</title>").unwrap();
        let title = title_regex
            .captures(&body)
            .and_then(|c| c.name("title"))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        // Simple link extraction (href attributes)
        let link_regex = Regex::new(r#"href\s*=\s*[\"']([^\"']+)[\"']"#).unwrap();
        let mut extracted_links = Vec::new();
        for cap in link_regex.captures_iter(&body) {
            if let Some(m) = cap.get(1) {
                let link = m.as_str().to_string();
                if !link.is_empty() && !link.starts_with("javascript:") {
                    if let Some(resolved) = self.resolve_url(base_url, &link) {
                        extracted_links.push(resolved);
                    }
                }
            }
        }
        (title, extracted_links)
    }
    // Helper function to fetch body and status
    async fn fetch_body(&self, url: &str) -> Result<(u16, String), FetcherError> {
        let request = Request::get(url)
            .header("Accept", HeaderValue::from_static("text/html"))
            .body(Body::empty())
            .expect("Failed to build request");
        let mut response =
            Client::new()
                .send(request)
                .await
                .map_err(|e| FetcherError::HttpFetchFailed {
                    url: url.to_string(),
                    status_code: 0,
                    message: format!("{:?}", e),
                })?;
        let status = response.status().as_u16();
        let body_bytes =
            response
                .body_mut()
                .contents()
                .await
                .map_err(|e| FetcherError::HttpFetchFailed {
                    url: url.to_string(),
                    status_code: status,
                    message: format!("{:?}", e),
                })?;
        let body = String::from_utf8_lossy(&body_bytes).to_string();
        Ok((status, body))
    }
}
