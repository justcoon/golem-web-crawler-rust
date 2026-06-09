use crate::common_lib::database::PostgresDbConfig;
use golem_rust::{ConfigSchema, Schema, agent_definition, agentic::Config, endpoint};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct OrchestratorConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct CrawlStatus {
    pub job_id: String,
    pub active_domains: Vec<String>,
    pub total_domains_crawl_count: u32,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum OrchestratorError {
    EmptySeedList,
    CrawlJobAlreadyActive { job_id: String },
    InvalidDomainRegistered { domain: String },
    OrchestratorDBFailure { message: String },
}

#[agent_definition(mount = "/crawlers/{crawl_job_id}")]
pub trait OrchestratorAgent {
    fn new(crawl_job_id: String, #[agent_config] config: Config<OrchestratorConfig>) -> Self;

    #[endpoint(post = "/start")]
    async fn start_crawl(&mut self, seeds: Vec<String>) -> Result<(), OrchestratorError>;

    #[endpoint(get = "/status")]
    async fn get_status(&self) -> Result<CrawlStatus, OrchestratorError>;

    #[endpoint(post = "/add_urls")]
    async fn add_urls(&mut self, urls: Vec<String>) -> Result<(), OrchestratorError>;
}
