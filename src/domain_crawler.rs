// src/domain_crawler.rs
use crate::common::PrioritizedUrl;
use crate::common_lib::database::PostgresDbConfig;
use crate::fetcher::FetcherAgentClient;
use golem_rust::wasip2::clocks::wall_clock::Datetime;
use golem_rust::{agent_definition, agent_implementation, agentic::Config, endpoint, ConfigSchema, Schema};
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
    pub is_active: bool,
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
    fn new(domain_name: String, #[agent_config] config: Config<DomainCrawlerConfig>) -> Self;

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
    config: Config<DomainCrawlerConfig>,
    state: DomainState,
}

#[agent_implementation]
impl DomainCrawlerAgent for DomainCrawlerAgentImpl {
    fn new(domain_name: String, #[agent_config] config: Config<DomainCrawlerConfig>) -> Self {
        Self {
            config,
            state: DomainState {
                domain: domain_name,
                politeness_delay_ms: 1000, // Default 1 second
                pending_queue: Vec::new(),
                in_progress_count: 0,
                is_active: false,
            },
        }
    }

    async fn enqueue(&mut self, urls: Vec<PrioritizedUrl>) -> Result<(), DomainCrawlerError> {
        // 1. Validate all URLs first to ensure atomic enqueuing
        for prioritized_url in &urls {
            // Very simple check: URL should contain domain name
            if !prioritized_url.url.contains(&self.state.domain) {
                return Err(DomainCrawlerError::InvalidUrlForDomain {
                    url: prioritized_url.url.clone(),
                    domain: self.state.domain.clone(),
                });
            }
        }

        // 2. Once validated, add non-duplicate URLs to the queue
        for prioritized_url in urls {
            if !self.state.pending_queue.iter().any(|u| u.url == prioritized_url.url) {
                self.state.pending_queue.push(prioritized_url);
            }
        }

        // Sort by priority DESC
        self.state.pending_queue.sort_by_key(|u| -u.priority);

        // Start processing if not already active and we have items
        if !self.state.is_active && !self.state.pending_queue.is_empty() {
            self.state.is_active = true;
            let mut client = DomainCrawlerAgentClient::get(self.state.domain.clone());
            client.trigger_process_next();
        }

        Ok(())
    }

    async fn get_state(&self) -> Result<DomainState, DomainCrawlerError> {
        Ok(self.state.clone())
    }

    async fn set_delay(&mut self, delay_ms: u32) -> Result<(), DomainCrawlerError> {
        self.state.politeness_delay_ms = delay_ms;
        Ok(())
    }

    async fn process_next(&mut self) -> Result<(), DomainCrawlerError> {
        if self.state.pending_queue.is_empty() {
            self.state.is_active = false;
            return Ok(());
        }

        // Pop the highest priority URL (which is at the end if sorted, but we sorted it DESC so it's at the end or we can just pop)
        // Wait, self.state.pending_queue.sort_by_key(|u| -u.priority) sorts DESC, so:
        // High priority first in the vec, e.g., index 0. To pop high priority, we should remove from index 0,
        // or sort ASC and pop from the end. Let's remove from index 0 for DESC, or sort ASC.
        // Let's sort ASC so pop() returns the highest priority:
        // pending_queue.sort_by_key(|u| u.priority);
        // Let's adjust sorting so pop() takes the highest priority URL from the end:
        self.state.pending_queue.sort_by_key(|u| u.priority);
        let target = match self.state.pending_queue.pop() {
            Some(t) => t,
            None => {
                self.state.is_active = false;
                return Ok(());
            }
        };

        self.state.in_progress_count += 1;

        // Fetch using the FetcherAgent worker
        let fetcher = FetcherAgentClient::get();
        let fetch_result = fetcher.fetch_and_parse(target.url.clone()).await;

        self.state.in_progress_count = self.state.in_progress_count.saturating_sub(1);

        match fetch_result {
            Ok(result) => {
                // Parse extracted links and enqueue domain-specific ones
                let mut domain_links = Vec::new();
                for link in result.extracted_links {
                    if link.url.contains(&self.state.domain) {
                        domain_links.push(link);
                    }
                }
                // Enqueue will add to queue and trigger another process_next if needed
                let _ = self.enqueue(domain_links).await;
            }
            Err(e) => {
                log::error!("Failed to fetch URL {}: {:?}", target.url, e);
            }
        }

        // Schedule next URL processing if still active and queue has items
        if self.state.is_active && !self.state.pending_queue.is_empty() {
            let delay_ms = self.state.politeness_delay_ms;
            let delay_seconds = (delay_ms as f64 / 1000.0).ceil() as u64;
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let mut client = DomainCrawlerAgentClient::get(self.state.domain.clone());
            client.schedule_process_next(Datetime {
                seconds: now_secs + delay_seconds,
                nanoseconds: 0,
            });
        } else {
            self.state.is_active = false;
        }

        Ok(())
    }
}
