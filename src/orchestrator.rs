use crate::common::PrioritizedUrl;
use crate::common_lib::database::{DatabaseHelper, PostgresDbConfig, Single};
use crate::domain_crawler::DomainCrawlerAgentClient;
use crate::encode_params;
use golem_rust::{
    ConfigSchema, Schema, agent_definition, agent_implementation, agentic::Config, endpoint,
};
use serde::{Deserialize, Serialize};

use crate::domain_crawler::UrlProcessingConfig;

#[derive(ConfigSchema)]
pub struct OrchestratorConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
    #[config_schema(nested)]
    pub url_processing: UrlProcessingConfig,
}

use crate::common::{FilterType, LinkFilter};

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum OrchestratorError {
    EmptySeedList,
    InvalidUrl { url: String },
    DbError { message: String },
}

#[agent_definition(ephemeral, mount = "/crawler")]
pub trait OrchestratorAgent {
    fn new(#[agent_config] config: Config<OrchestratorConfig>) -> Self;

    #[endpoint(post = "/start")]
    async fn start_crawl(&self, seeds: Vec<String>) -> Result<(), OrchestratorError>;

    #[endpoint(get = "/domains")]
    async fn get_domains(&self) -> Result<Vec<String>, OrchestratorError>;

    #[endpoint(post = "/filters")]
    async fn add_filter(
        &self,
        pattern: String,
        filter_type: FilterType,
    ) -> Result<(), OrchestratorError>;

    #[endpoint(get = "/filters")]
    async fn get_filters(&self) -> Result<Vec<LinkFilter>, OrchestratorError>;

    #[endpoint(delete = "/filters/{id}")]
    async fn delete_filter(&self, id: i32) -> Result<(), OrchestratorError>;
}

pub struct OrchestratorAgentImpl {
    config: Config<OrchestratorConfig>,
}

#[agent_implementation]
impl OrchestratorAgent for OrchestratorAgentImpl {
    fn new(#[agent_config] config: Config<OrchestratorConfig>) -> Self {
        Self { config }
    }

    async fn start_crawl(&self, seeds: Vec<String>) -> Result<(), OrchestratorError> {
        if seeds.is_empty() {
            return Err(OrchestratorError::EmptySeedList);
        }

        let normalize_prefixes = self.config.get().url_processing.normalize_prefixes.get();

        // Group seed URLs by domain
        let mut grouped: std::collections::HashMap<String, Vec<PrioritizedUrl>> =
            std::collections::HashMap::new();
        for url in seeds {
            if let Ok(parsed_url) = url::Url::parse(&url) {
                if let Some(domain) = parsed_url.host_str() {
                    let normalized_domain =
                        crate::common::normalize_domain(domain, &normalize_prefixes);
                    grouped
                        .entry(normalized_domain)
                        .or_default()
                        .push(PrioritizedUrl {
                            url: parsed_url,
                            priority: 10, // Default seed priority
                        });
                } else {
                    return Err(OrchestratorError::InvalidUrl { url });
                }
            } else {
                return Err(OrchestratorError::InvalidUrl { url });
            }
        }

        // Forward to respective DomainCrawlerAgents asynchronously
        for (domain, urls) in grouped {
            let mut client = DomainCrawlerAgentClient::get(domain);
            client.trigger_enqueue(urls);
        }

        Ok(())
    }

    async fn get_domains(&self) -> Result<Vec<String>, OrchestratorError> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        let rows: Vec<Single<String>> = db_helper
            .transactional(|tx| {
                let sql = "SELECT DISTINCT domain FROM page_contents ORDER BY domain ASC";
                let res = tx.query(sql, vec![])?;
                use crate::common_lib::database::DbResultDecoder;
                Single::<String>::decode_result(res)
            })
            .map_err(|e| OrchestratorError::DbError {
                message: format!("Failed to query domains: {:?}", e),
            })?;

        Ok(rows.into_iter().map(|s| s.0).collect())
    }

    async fn add_filter(
        &self,
        pattern: String,
        filter_type: FilterType,
    ) -> Result<(), OrchestratorError> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        db_helper
            .transactional(|tx| {
                let sql = "INSERT INTO link_filters (pattern, filter_type) VALUES ($1, $2) \
                           ON CONFLICT (pattern) DO UPDATE SET filter_type = EXCLUDED.filter_type, is_active = true";
                tx.execute(sql, encode_params!(&pattern, filter_type))?;
                Ok(())
            })
            .map_err(|e| OrchestratorError::DbError {
                message: format!("Failed to add link filter: {:?}", e),
            })?;

        Ok(())
    }

    async fn get_filters(&self) -> Result<Vec<LinkFilter>, OrchestratorError> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        let rows = db_helper
            .transactional(|tx| {
                let sql = "SELECT id, pattern, filter_type, is_active, created_at::TEXT AS created_at \
                           FROM link_filters ORDER BY id DESC";
                let res = tx.query(sql, vec![])?;
                use crate::common_lib::database::DbResultDecoder;
                LinkFilter::decode_result(res)
            })
            .map_err(|e| OrchestratorError::DbError {
                message: format!("Failed to query link filters: {:?}", e),
            })?;

        Ok(rows)
    }

    async fn delete_filter(&self, id: i32) -> Result<(), OrchestratorError> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        db_helper
            .transactional(|tx| {
                let sql = "DELETE FROM link_filters WHERE id = $1";
                tx.execute(sql, encode_params!(&id))?;
                Ok(())
            })
            .map_err(|e| OrchestratorError::DbError {
                message: format!("Failed to delete link filter: {:?}", e),
            })?;

        Ok(())
    }
}
