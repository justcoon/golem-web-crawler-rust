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
    pub url: url::Url,
    pub title: String,
    pub extracted_links: Vec<url::Url>,
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
    DbError {
        message: String,
    },
}

#[agent_definition(ephemeral)]
pub trait FetcherAgent {
    fn new(#[agent_config] config: Config<FetcherConfig>) -> Self;
    async fn fetch_and_parse(&self, url: url::Url) -> Result<FetchResult, FetcherError>;
}

pub struct FetcherAgentImpl {
    config: Config<FetcherConfig>,
}

#[agent_implementation]
impl FetcherAgent for FetcherAgentImpl {
    fn new(#[agent_config] config: Config<FetcherConfig>) -> Self {
        Self { config }
    }

    async fn fetch_and_parse(&self, url: url::Url) -> Result<FetchResult, FetcherError> {
        let url_str = url.to_string();

        // Perform HTTP GET and extract body using helper
        let (status, body) = fetch_body(&url_str).await?;

        let cfg = self.config.get();
        let db_helper = DatabaseHelper::from(cfg.db).map_err(|e| FetcherError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        // Retrieve active filters
        let active_filters = db_helper
            .transactional(|tx| {
                let sql = "SELECT pattern, filter_type FROM link_filters WHERE is_active = true";
                let res = tx.query(sql, vec![])?;
                use crate::common_lib::database::DbResultDecoder;
                <(String, crate::common::FilterType)>::decode_result(res)
            })
            .map_err(|e| FetcherError::DbError {
                message: format!("Failed to load link filters: {:?}", e),
            })?;

        // Extract title and links using helper
        let (title, extracted_links) = extract_content(&url, &body, &active_filters);

        // Persist result to PostgreSQL
        let domain = url.host_str().unwrap_or_default().to_string();
        db_helper
            .transactional(|tx| {
                let sql = "INSERT INTO page_contents (url, domain, title, http_status, raw_html, extracted_text) \
                           VALUES ($1, $2, $3, $4, $5, $6) \
                           ON CONFLICT (url) DO UPDATE SET \
                           domain = EXCLUDED.domain, \
                           title = EXCLUDED.title, \
                           http_status = EXCLUDED.http_status, \
                           raw_html = EXCLUDED.raw_html, \
                           extracted_text = EXCLUDED.extracted_text, \
                           saved_at = CURRENT_TIMESTAMP";
                tx.execute(sql, encode_params!(&url_str, &domain, &title, &(status as i32), &body, &body))?;
                Ok(())
            })
            .map_err(|e| FetcherError::DbError {
                message: format!("Failed to write page contents: {:?}", e),
            })?;

        Ok(FetchResult {
            url,
            title,
            extracted_links,
            status,
        })
    }
}

// Helper function to fetch body and status, following redirects and adding a User-Agent
async fn fetch_body(url: &str) -> Result<(u16, String), FetcherError> {
    let mut current_url = url.to_string();
    let mut redirect_count = 0;
    const MAX_REDIRECTS: u8 = 5;

    loop {
        let request = Request::get(&current_url)
            .header("Accept", HeaderValue::from_static("text/html"))
            .header("User-Agent", HeaderValue::from_static("golem-crawler/1.0"))
            .body(Body::empty())
            .expect("Failed to build request");

        let mut response =
            Client::new()
                .send(request)
                .await
                .map_err(|e| FetcherError::HttpFetchFailed {
                    url: current_url.clone(),
                    status_code: 0,
                    message: format!("{:?}", e),
                })?;

        let status = response.status().as_u16();

        // If it is a redirect, resolve location and follow it
        if (300..=399).contains(&status) {
            if redirect_count >= MAX_REDIRECTS {
                return Err(FetcherError::HttpFetchFailed {
                    url: current_url.clone(),
                    status_code: status,
                    message: "Too many redirects".to_string(),
                });
            }

            if let Some(loc_header) = response.headers().get("location") {
                let loc_str = loc_header
                    .to_str()
                    .map_err(|e| FetcherError::HttpFetchFailed {
                        url: current_url.clone(),
                        status_code: status,
                        message: format!("Invalid Location header encoding: {:?}", e),
                    })?;

                let base = url::Url::parse(&current_url).map_err(|e| FetcherError::InvalidUrl {
                    url: current_url.clone(),
                    reason: format!("Failed to parse current URL: {:?}", e),
                })?;

                let next_url = base.join(loc_str).map_err(|e| FetcherError::InvalidUrl {
                    url: loc_str.to_string(),
                    reason: format!("Failed to resolve redirect location: {:?}", e),
                })?;

                current_url = next_url.to_string();
                redirect_count += 1;
                log::info!("Following redirect to: {} (status {})", current_url, status);
                continue;
            }
        }

        let body_bytes =
            response
                .body_mut()
                .contents()
                .await
                .map_err(|e| FetcherError::HttpFetchFailed {
                    url: current_url.clone(),
                    status_code: status,
                    message: format!("{:?}", e),
                })?;
        let body = String::from_utf8_lossy(body_bytes).to_string();
        return Ok((status, body));
    }
}

fn resolve_url(base_url: &url::Url, relative: &str) -> Option<url::Url> {
    base_url.join(relative).ok()
}

fn is_filtered(url: &url::Url, filters: &[(String, crate::common::FilterType)]) -> bool {
    let url_str = url.to_string();
    let host = url.host_str().unwrap_or_default().to_lowercase();
    let path = url.path().to_lowercase();

    for (pattern, filter_type) in filters {
        let pattern_lower = pattern.to_lowercase();
        match filter_type {
            crate::common::FilterType::DomainBlacklist => {
                if host == pattern_lower || host.ends_with(&format!(".{}", pattern_lower)) {
                    return true;
                }
            }
            crate::common::FilterType::Keyword => {
                if url_str.to_lowercase().contains(&pattern_lower) {
                    return true;
                }
            }
            crate::common::FilterType::UrlRegex => {
                if let Ok(re) = Regex::new(pattern) {
                    if re.is_match(&url_str) {
                        return true;
                    }
                }
            }
            crate::common::FilterType::Extension => {
                if path.ends_with(&pattern_lower) {
                    return true;
                }
            }
        }
    }
    false
}

fn extract_content(
    base_url: &url::Url,
    body: &str,
    active_filters: &[(String, crate::common::FilterType)],
) -> (String, Vec<url::Url>) {
    // Extract title using regex (case-insensitive)
    let title_regex = Regex::new(r"(?i)<title>(?P<title>.*?)</title>").unwrap();
    let title = title_regex
        .captures(body)
        .and_then(|c| c.name("title"))
        .map(|m| m.as_str().trim().to_string())
        .unwrap_or_default();

    // Check for a <base href="..."> tag in the HTML body to determine resolution context
    let base_tag_regex = Regex::new(r#"(?i)<base\s+[^>]*href\s*=\s*["']([^"']+)["']"#).unwrap();
    let resolved_base_url = if let Some(cap) = base_tag_regex.captures(body) {
        if let Some(m) = cap.get(1) {
            let base_href = m.as_str().trim();
            if let Ok(parsed_base) = url::Url::parse(base_href) {
                parsed_base
            } else if let Ok(joined_base) = base_url.join(base_href) {
                joined_base
            } else {
                base_url.clone()
            }
        } else {
            base_url.clone()
        }
    } else {
        base_url.clone()
    };

    // Regex matching href attributes inside anchor (<a>) tags specifically
    let link_regex = Regex::new(r#"(?i)<a\s+[^>]*href\s*=\s*["']([^"']+)["']"#).unwrap();
    let mut extracted_links = Vec::new();
    for cap in link_regex.captures_iter(body) {
        if let Some(m) = cap.get(1) {
            let link = m.as_str().trim().to_string();
            if !link.is_empty()
                && !link.starts_with('#')
                && !link.starts_with("javascript:")
                && !link.starts_with("mailto:")
                && !link.starts_with("tel:")
                && let Some(resolved) = resolve_url(&resolved_base_url, &link)
            {
                let scheme = resolved.scheme();
                if (scheme == "http" || scheme == "https")
                    && !is_filtered(&resolved, active_filters)
                {
                    extracted_links.push(resolved);
                }
            }
        }
    }
    (title, extracted_links)
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_extract_content_filters() {
        let base_url = Url::parse("https://example.com/page").unwrap();
        let body = r##"
            <html>
            <head>
                <title>Test Page</title>
                <link rel="stylesheet" href="/assets/style.css">
                <link rel="icon" href="https://example.com/favicon.ico">
            </head>
            <body>
                <a href="/about">About Us</a>
                <a href="https://other.com/contact.html">Contact</a>
                <a href="javascript:void(0)">JS Link</a>
                <a href="#section">Fragment Link</a>
                <a href="mailto:info@example.com">Email Us</a>
                <a href="/downloads/report.pdf">Download PDF</a>
                <a href="/static/react.bundle.js">JS Library</a>
                <img src="/img/logo.png" href="/img/logo.png">
                <a href="https://facebook.com/someprofile">Facebook Profile</a>
                <a href="https://other.com/page?utm_campaign=xyz">Campaign Link</a>
            </body>
            </html>
        "##;

        let active_filters = vec![
            (
                "facebook.com".to_string(),
                crate::common::FilterType::DomainBlacklist,
            ),
            ("utm_".to_string(), crate::common::FilterType::Keyword),
            (".css".to_string(), crate::common::FilterType::Extension),
            (".ico".to_string(), crate::common::FilterType::Extension),
            (".pdf".to_string(), crate::common::FilterType::Extension),
            (".js".to_string(), crate::common::FilterType::Extension),
        ];

        let (title, links) = extract_content(&base_url, body, &active_filters);
        assert_eq!(title, "Test Page");

        let expected_links: Vec<Url> = vec![
            Url::parse("https://example.com/about").unwrap(),
            Url::parse("https://other.com/contact.html").unwrap(),
        ];
        assert_eq!(links, expected_links);
    }

    #[test]
    fn test_extract_content_with_base_tag() {
        let base_url = Url::parse("https://example.com/blog/post1").unwrap();
        let body = r##"
            <html>
            <head>
                <base href="https://example.com/archive/">
                <title>Base Tag Test</title>
            </head>
            <body>
                <a href="about">About (relative to archive)</a>
                <a href="/root">Root-relative</a>
                <a href="https://external.com">External</a>
            </body>
            </html>
        "##;

        let (title, links) = extract_content(&base_url, body, &[]);
        assert_eq!(title, "Base Tag Test");
        let expected_links: Vec<Url> = vec![
            Url::parse("https://example.com/archive/about").unwrap(),
            Url::parse("https://example.com/root").unwrap(),
            Url::parse("https://external.com/").unwrap(),
        ];
        assert_eq!(links, expected_links);
    }
}
