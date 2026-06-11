// src/domain_crawler.rs
use crate::common::{PrioritizedUrl, get_domain_from_url};
use crate::fetcher::FetcherAgentClient;
use golem_rust::agentic::{Config, Secret};
use golem_rust::wasip2::clocks::wall_clock::Datetime;
use golem_rust::{ConfigSchema, Schema, agent_definition, agent_implementation, endpoint};
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
    #[config_schema(secret)]
    pub allow_cross_domain: Secret<bool>,
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
    pub robots_disallowed: Option<Vec<String>>,
}

impl DomainState {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            politeness_delay_ms: 1000,
            pending_queue: Vec::new(),
            status: ProcessingStatus::Inactive,
            robots_disallowed: None,
        }
    }

    pub fn validate_url(&self, url: &url::Url) -> bool {
        url.host_str().map(|d| d == self.domain).unwrap_or(false)
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

    pub fn is_allowed_by_robots(&self, url: &url::Url) -> bool {
        if let Some(ref disallowed_rules) = self.robots_disallowed {
            let path = url.path();
            let is_disallowed = disallowed_rules.iter().any(|prefix| {
                if prefix == "/" {
                    true
                } else {
                    path.starts_with(prefix)
                }
            });
            return !is_disallowed;
        }
        true
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
                    url: prioritized_url.url.to_string(),
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

        // 1. Ensure robots.txt rules are fetched and cached
        if self.state.robots_disallowed.is_none() {
            log::info!("Fetching robots.txt for domain: {}", self.state.domain);
            let (disallowed, delay_ms) = self.fetch_and_parse_robots().await;
            self.state.robots_disallowed = Some(disallowed);
            if let Some(delay) = delay_ms {
                log::info!("Setting politeness delay to {}ms from Crawl-delay", delay);
                self.state.set_delay(delay);
            }
        }

        let target = match self.state.get_next_url() {
            Some(t) => t,
            None => {
                self.state.set_status(ProcessingStatus::Inactive);
                return Ok(());
            }
        };

        // 2. Check compliance with robots.txt rules
        if !self.state.is_allowed_by_robots(&target.url) {
            log::info!("URL disallowed by robots.txt, skipping: {}", target.url);
            self.schedule_next_step();
            return Ok(());
        }

        self.state.set_status(ProcessingStatus::Processing);

        // Fetch using the FetcherAgent worker
        let fetcher = FetcherAgentClient::new_phantom();
        let fetch_result = fetcher.fetch_and_parse(target.url.clone()).await;

        let config = self.config.get();
        let max_url_len = config.url_processing.max_url_length.get();
        let boost_words = config.url_processing.boost_words.get();
        let allow_cross_domain = config.url_processing.allow_cross_domain.get();

        match fetch_result {
            Ok(result) => {
                // Group extracted links by domain and route to specific domain agents
                let mut grouped: std::collections::HashMap<String, Vec<PrioritizedUrl>> =
                    std::collections::HashMap::new();
                for link_url in result.extracted_links {
                    let link_str = link_url.to_string();
                    if link_str.len() as u32 > max_url_len {
                        continue;
                    }
                    if let Some(domain) = get_domain_from_url(&link_str)
                        && (domain == self.state.domain || allow_cross_domain)
                    {
                        let priority = calculate_priority(&link_str, &boost_words);
                        grouped.entry(domain).or_default().push(PrioritizedUrl {
                            url: link_url,
                            priority,
                        });
                    }
                }

                for (domain, urls) in grouped {
                    if domain == self.state.domain {
                        // Filter local URLs against our already cached robots.txt
                        let filtered_urls = urls
                            .into_iter()
                            .filter(|u| self.state.is_allowed_by_robots(&u.url))
                            .collect();
                        self.state.add_urls(filtered_urls);
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

        self.schedule_next_step();
        Ok(())
    }
}

impl DomainCrawlerAgentImpl {
    async fn fetch_and_parse_robots(&self) -> (Vec<String>, Option<u32>) {
        let robots_url = format!("https://{}/robots.txt", self.state.domain);
        let client = golem_wasi_http::Client::new();
        let response = match client.get(&robots_url).send() {
            Ok(resp) => resp,
            Err(e) => {
                log::warn!(
                    "Failed to fetch robots.txt for {}: {:?}",
                    self.state.domain,
                    e
                );
                return (Vec::new(), None);
            }
        };

        if response.status().as_u16() != 200 {
            return (Vec::new(), None);
        }

        let body = match response.text() {
            Ok(t) => t,
            Err(_) => return (Vec::new(), None),
        };

        parse_robots_txt(&body)
    }

    fn schedule_next_step(&mut self) {
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
    }
}

fn parse_robots_txt(content: &str) -> (Vec<String>, Option<u32>) {
    let mut disallowed = Vec::new();
    let mut crawl_delay = None;
    let mut in_relevant_agent = false;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.splitn(2, ':').collect();
        if parts.len() < 2 {
            continue;
        }

        let key = parts[0].trim().to_lowercase();
        let val = parts[1].trim();

        if key == "user-agent" {
            let agent = val.to_lowercase();
            // We target '*' or 'golem'
            in_relevant_agent = agent == "*" || agent == "golem";
        } else if in_relevant_agent {
            if key == "disallow" {
                if !val.is_empty() {
                    disallowed.push(val.to_string());
                }
            } else if key == "crawl-delay" {
                if let Ok(secs) = val.parse::<f64>() {
                    crawl_delay = Some((secs * 1000.0) as u32);
                }
            }
        }
    }

    (disallowed, crawl_delay)
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
