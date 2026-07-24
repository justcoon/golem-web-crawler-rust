use crate::common::{FilterType, LinkFilter, PrioritizedUrl, UrlProcessingConfig};
use crate::common_lib::database::{DatabaseHelper, PostgresDbConfig};
use crate::domain_crawler::DomainCrawlerAgentClient;
use crate::encode_params;
use golem_rust::{
    ConfigSchema, Schema, agent_definition, agent_implementation, agentic::Config, endpoint,
};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct OrchestratorConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
    #[config_schema(nested)]
    pub url_processing: UrlProcessingConfig,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct DomainInfo {
    pub domain: String,
    pub page_count: u64,
}

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
    async fn get_domains(&self) -> Result<Vec<DomainInfo>, OrchestratorError>;

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
            Err(OrchestratorError::EmptySeedList)
        } else {
            let normalize_prefixes = self.config.get().url_processing.normalize_prefixes.get();

            // Parse and normalize seed URLs
            let mut parsed_urls = Vec::new();
            for url in seeds {
                let parsed_url = url::Url::parse(&url)
                    .map_err(|_| OrchestratorError::InvalidUrl { url: url.clone() })?;
                let normalized_url =
                    crate::common::normalize_url_domain(&parsed_url, &normalize_prefixes);
                parsed_urls.push(normalized_url);
            }

            // Group and prioritize seed URLs by domain
            let grouped_by_domain = crate::common::group_prioritized_urls_by_domain(
                parsed_urls,
                |u| PrioritizedUrl {
                    url: u,
                    priority: 10, // Default seed priority
                },
            );

            // Forward to respective DomainCrawlerAgents asynchronously
            for (domain, prioritized_urls) in grouped_by_domain {
                let mut client = DomainCrawlerAgentClient::get(domain);
                client.trigger_enqueue(prioritized_urls);
            }

            Ok(())
        }
    }

    async fn get_domains(&self) -> Result<Vec<DomainInfo>, OrchestratorError> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DbError {
            message: format!("Failed to connect to database: {:?}", e),
        })?;

        let rows: Vec<(String, i64)> = db_helper
            .transactional(|tx| {
                let sql = "SELECT domain, COUNT(*) FROM page_contents GROUP BY domain ORDER BY domain ASC";
                let res = tx.query(sql, vec![])?;
                use crate::common_lib::database::DbResultDecoder;
                <(String, i64)>::decode_result(res)
            })
            .map_err(|e| OrchestratorError::DbError {
                message: format!("Failed to query domains: {:?}", e),
            })?;

        Ok(rows
            .into_iter()
            .map(|(domain, page_count)| DomainInfo {
                domain,
                page_count: page_count as u64,
            })
            .collect())
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
