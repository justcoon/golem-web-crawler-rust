// src/domain_crawler.rs
use crate::common::PrioritizedUrl;
use crate::common_lib::database::PostgresDbConfig;
use golem_rust::{ConfigSchema, Schema, agent_definition, agentic::Config, endpoint};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct DomainCrawlerConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct DomainState {
    pub domain: String,
    pub politeness_delay_ms: u32,
    // Queue of pending URLs sorted by priority DESC
    pub pending_queue: Vec<PrioritizedUrl>,
    pub in_progress_count: u32,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum DomainCrawlerError {
    QueueFull { max_size: usize },
    InvalidUrlForDomain { url: String, domain: String },
    ConfigurationError { message: String },
}

#[agent_definition(mount = "/domains/{domain_name}")]
pub trait DomainCrawlerAgent {
    // Constructor identifies the domain crawler instance.
    fn new(domain_name: String, #[agent_config] config: Config<DomainCrawlerConfig>) -> Self;

    // Enqueue new URLs discovered under this domain.
    async fn enqueue(&mut self, urls: Vec<PrioritizedUrl>) -> Result<(), DomainCrawlerError>;

    // Retrieve current queue and state.
    #[endpoint(get = "/state")]
    async fn get_state(&self) -> Result<DomainState, DomainCrawlerError>;

    // Adjust politeness delay via REST.
    #[endpoint(post = "/config/delay")]
    async fn set_delay(&mut self, delay_ms: u32) -> Result<(), DomainCrawlerError>;
}
