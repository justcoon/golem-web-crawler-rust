use crate::common::{PrioritizedUrl, get_domain_from_url};
use crate::common_lib::database::{DatabaseHelper, PostgresDbConfig, Single};
use crate::domain_crawler::DomainCrawlerAgentClient;
use golem_rust::{
    ConfigSchema, Schema, agent_definition, agent_implementation, agentic::Config, endpoint,
};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct OrchestratorConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum OrchestratorError {
    EmptySeedList,
    InvalidUrl { url: String },
    DatabaseError { message: String },
}

#[agent_definition(ephemeral, mount = "/crawler")]
pub trait OrchestratorAgent {
    fn new(#[agent_config] config: Config<OrchestratorConfig>) -> Self;

    #[endpoint(post = "/start")]
    async fn start_crawl(&self, seeds: Vec<String>) -> Result<(), OrchestratorError>;

    #[endpoint(get = "/domains")]
    async fn get_domains(&self) -> Result<Vec<String>, OrchestratorError>;
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

        // Group seed URLs by domain
        let mut grouped: std::collections::HashMap<String, Vec<PrioritizedUrl>> =
            std::collections::HashMap::new();
        for url in seeds {
            if let Some(domain) = get_domain_from_url(&url) {
                if let Ok(parsed_url) = url::Url::parse(&url) {
                    grouped.entry(domain).or_default().push(PrioritizedUrl {
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
        let db_helper =
            DatabaseHelper::from(db_cfg).map_err(|e| OrchestratorError::DatabaseError {
                message: format!("Failed to connect to database: {:?}", e),
            })?;

        let rows: Vec<Single<String>> = db_helper
            .transactional(|tx| {
                let sql = "SELECT DISTINCT domain FROM page_contents ORDER BY domain ASC";
                let res = tx.query(sql, vec![])?;
                use crate::common_lib::database::DbResultDecoder;
                Single::<String>::decode_result(res)
            })
            .map_err(|e| OrchestratorError::DatabaseError {
                message: format!("Failed to query domains: {:?}", e),
            })?;

        Ok(rows.into_iter().map(|s| s.0).collect())
    }
}
