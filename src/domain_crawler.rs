// src/domain_crawler.rs
use crate::common::{PrioritizedUrl, get_domain_from_url};
use crate::fetcher::FetcherAgentClient;
use golem_rust::wasip2::clocks::wall_clock::Datetime;
use golem_rust::{ConfigSchema, Schema, agent_definition, agent_implementation, endpoint};
use golem_rust::agentic::{Config, Secret};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct DomainCrawlerConfig {
    #[config_schema(nested)]
    pub url_processing: UrlProcessingConfig,
}

#[derive(ConfigSchema)]
pub struct UrlProcessingConfig {
    #[config_schema(secret)]
    pub boost_words: Secret<Vec<String>>,
    #[config_schema(secret)]
    pub max_url_length: Secret<u32>,
}

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
        get_domain_from_url(url)
            .map(|d| d == self.domain)
            .unwrap_or(false)
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
    state: DomainState,
    config: Config<DomainCrawlerConfig>,
}

#[agent_implementation]
impl DomainCrawlerAgent for DomainCrawlerAgentImpl {
    fn new(domain_name: String, #[agent_config] config: Config<DomainCrawlerConfig>) -> Self {
        Self {
            state: DomainState::new(domain_name),
            config,
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
        let fetcher = FetcherAgentClient::new_phantom();
        let fetch_result = fetcher.fetch_and_parse(target.url.clone()).await;

        let config = self.config.get();
        let max_url_len = config.url_processing.max_url_length.get();
        let boost_words = config.url_processing.boost_words.get();

        match fetch_result {
            Ok(result) => {
                // Group extracted links by domain and route to specific domain agents
                let mut grouped: std::collections::HashMap<String, Vec<PrioritizedUrl>> =
                    std::collections::HashMap::new();
                for link in result.extracted_links {
                    if link.len() as u32 > max_url_len {
                        continue;
                    }
                    if let Some(domain) = get_domain_from_url(&link) {
                        let priority = calculate_priority(&link, &boost_words);
                        grouped.entry(domain).or_default().push(PrioritizedUrl {
                            url: link,
                            priority,
                        });
                    }
                }

                for (domain, urls) in grouped {
                    if domain == self.state.domain {
                        self.state.add_urls(urls);
                    } else {
                        let mut client = DomainCrawlerAgentClient::get(domain);
                        client.trigger_enqueue(urls);
                    }
                }
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

fn calculate_priority(url: &str, boost_words: &[String]) -> i32 {
    let mut priority = 10;

    // Path depth penalty (e.g. -1 for each path segment)
    if let Some(path_idx) = url.find("://") {
        let path_part = &url[path_idx + 3..];
        if let Some(slash_idx) = path_part.find('/') {
            let segments = path_part[slash_idx..]
                .split('/')
                .filter(|s| !s.is_empty())
                .count();
            priority -= segments as i32;
        }
    }

    // Query parameter penalty
    if url.contains('?') {
        priority -= 2;
    }

    // Keyword boosts
    for word in boost_words {
        if url.contains(word) {
            priority += 2;
            break;
        }
    }

    priority
}
