// src/domain_crawler.rs
use crate::common::PrioritizedUrl;
use crate::common_lib::database::PostgresDbConfig;
use crate::fetcher::FetcherAgentClient;
use golem_rust::wasip2::clocks::wall_clock::Datetime;
use golem_rust::{
    ConfigSchema, Schema, agent_definition, agent_implementation, agentic::Config, endpoint,
};
use serde::{Deserialize, Serialize};

// #[derive(ConfigSchema)]
// pub struct DomainCrawlerConfig {
//     #[config_schema(nested)]
//     pub db: PostgresDbConfig,
// }

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum ProcessingStatus {
    Inactive,
    Scheduled,
    Processing,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct DomainState {
    pub domain: String,
    pub politeness_delay_ms: u32,
    // Queue of pending URLs sorted by priority ASC (so pop() returns highest priority)
    pub pending_queue: Vec<PrioritizedUrl>,
    pub status: ProcessingStatus,
}

impl DomainState {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            politeness_delay_ms: 1000,
            pending_queue: Vec::new(),
            status: ProcessingStatus::Inactive,
        }
    }

    pub fn validate_url(&self, url: &str) -> bool {
        url.contains(&self.domain)
    }

    pub fn add_urls(&mut self, urls: Vec<PrioritizedUrl>) {
        for prioritized_url in urls {
            if !self
                .pending_queue
                .iter()
                .any(|u| u.url == prioritized_url.url)
            {
                self.pending_queue.push(prioritized_url);
            }
        }
        // Sort by priority ASC so pop() returns highest priority
        self.pending_queue.sort_by_key(|u| u.priority);
    }

    pub fn has_pending(&self) -> bool {
        !self.pending_queue.is_empty()
    }

    pub fn get_next_url(&mut self) -> Option<PrioritizedUrl> {
        self.pending_queue.pop()
    }

    pub fn set_status(&mut self, status: ProcessingStatus) {
        self.status = status;
    }

    pub fn get_status(&self) -> &ProcessingStatus {
        &self.status
    }

    pub fn is_processing(&self) -> bool {
        matches!(self.status, ProcessingStatus::Processing)
    }

    pub fn is_scheduled(&self) -> bool {
        matches!(self.status, ProcessingStatus::Scheduled)
    }

    pub fn is_inactive(&self) -> bool {
        matches!(self.status, ProcessingStatus::Inactive)
    }

    pub fn set_delay(&mut self, delay_ms: u32) {
        self.politeness_delay_ms = delay_ms;
    }

    pub fn get_delay(&self) -> u32 {
        self.politeness_delay_ms
    }
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum DomainCrawlerError {
    QueueFull { max_size: usize },
    InvalidUrlForDomain { url: String, domain: String },
    ConfigurationError { message: String },
    FetcherFailed { message: String },
}

#[agent_definition(mount = "/domains/{domain_name}")]
pub trait DomainCrawlerAgent {
    // Constructor identifies the domain crawler instance.
    fn new(domain_name: String) -> Self;

    // Enqueue new URLs discovered under this domain.
    async fn enqueue(&mut self, urls: Vec<PrioritizedUrl>) -> Result<(), DomainCrawlerError>;

    // Retrieve current queue and state.
    #[endpoint(get = "/state")]
    async fn get_state(&self) -> Result<DomainState, DomainCrawlerError>;

    // Adjust politeness delay via REST.
    #[endpoint(post = "/config/delay")]
    async fn set_delay(&mut self, delay_ms: u32) -> Result<(), DomainCrawlerError>;

    // Processes the next URL in the queue (internal loop step).
    async fn process_next(&mut self) -> Result<(), DomainCrawlerError>;
}

pub struct DomainCrawlerAgentImpl {
    state: DomainState,
}

#[agent_implementation]
impl DomainCrawlerAgent for DomainCrawlerAgentImpl {
    fn new(domain_name: String) -> Self {
        Self {
            state: DomainState::new(domain_name),
        }
    }

    async fn enqueue(&mut self, urls: Vec<PrioritizedUrl>) -> Result<(), DomainCrawlerError> {
        // Validate all URLs first to ensure atomic enqueuing
        for prioritized_url in &urls {
            if !self.state.validate_url(&prioritized_url.url) {
                return Err(DomainCrawlerError::InvalidUrlForDomain {
                    url: prioritized_url.url.clone(),
                    domain: self.state.domain.clone(),
                });
            }
        }

        // Add URLs to the queue (handles duplicates and sorting)
        self.state.add_urls(urls);

        // Start processing if inactive and we have items
        if self.state.is_inactive() && self.state.has_pending() {
            self.state.set_status(ProcessingStatus::Scheduled);
            let mut client = DomainCrawlerAgentClient::get(self.state.domain.clone());
            client.trigger_process_next();
        }

        Ok(())
    }

    async fn get_state(&self) -> Result<DomainState, DomainCrawlerError> {
        Ok(self.state.clone())
    }

    async fn set_delay(&mut self, delay_ms: u32) -> Result<(), DomainCrawlerError> {
        self.state.set_delay(delay_ms);
        Ok(())
    }

    async fn process_next(&mut self) -> Result<(), DomainCrawlerError> {
        if !self.state.has_pending() {
            self.state.set_status(ProcessingStatus::Inactive);
            return Ok(());
        }

        let target = match self.state.get_next_url() {
            Some(t) => t,
            None => {
                self.state.set_status(ProcessingStatus::Inactive);
                return Ok(());
            }
        };

        self.state.set_status(ProcessingStatus::Processing);

        // Fetch using the FetcherAgent worker
        let fetcher = FetcherAgentClient::get();
        let fetch_result = fetcher.fetch_and_parse(target.url.clone()).await;

        match fetch_result {
            Ok(result) => {
                // Filter and enqueue domain-specific links
                let domain_links: Vec<PrioritizedUrl> = result
                    .extracted_links
                    .into_iter()
                    .filter(|link| self.state.validate_url(&link.url))
                    .collect();

                let _ = self.enqueue(domain_links).await;
            }
            Err(e) => {
                log::error!("Failed to fetch URL {}: {:?}", target.url, e);
            }
        }

        // Schedule next URL processing if we have items
        if self.state.has_pending() {
            let delay_ms = self.state.get_delay();
            let delay_seconds = (delay_ms as f64 / 1000.0).ceil() as u64;
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            self.state.set_status(ProcessingStatus::Scheduled);
            let mut client = DomainCrawlerAgentClient::get(self.state.domain.clone());
            client.schedule_process_next(Datetime {
                seconds: now_secs + delay_seconds,
                nanoseconds: 0,
            });
        } else {
            self.state.set_status(ProcessingStatus::Inactive);
        }

        Ok(())
    }
}
