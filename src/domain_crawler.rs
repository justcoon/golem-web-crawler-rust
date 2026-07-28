// src/domain_crawler.rs
use crate::common::{PrioritizedUrl, UrlProcessingConfig};
use crate::common_lib::database::{DatabaseHelper, PostgresDbConfig, Single};
use crate::fetcher::FetcherAgentClient;
use golem_rust::agentic::Config;
use golem_rust::wasip2::clocks::wall_clock::Datetime;
use golem_rust::{ConfigSchema, Schema, agent_definition, agent_implementation, endpoint};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct DomainCrawlerConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
    #[config_schema(nested)]
    pub url_processing: UrlProcessingConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Schema, Serialize, Deserialize)]
pub enum CrossDomainPolicy {
    None,
    SubdomainsOnly,
    Any,
}

impl std::str::FromStr for CrossDomainPolicy {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "subdomainsonly" | "subdomains_only" => Ok(CrossDomainPolicy::SubdomainsOnly),
            "any" => Ok(CrossDomainPolicy::Any),
            _ => Ok(CrossDomainPolicy::None),
        }
    }
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum ProcessingStatus {
    Inactive,
    Scheduled,
    Processing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Schema, Serialize, Deserialize)]
pub enum PriorityBucket {
    High,
    Medium,
    Low,
}

impl PriorityBucket {
    pub fn from_priority(priority: i32) -> Self {
        if priority >= 10 {
            PriorityBucket::High
        } else if priority >= 5 {
            PriorityBucket::Medium
        } else {
            PriorityBucket::Low
        }
    }
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct DomainState {
    pub domain: String,
    pub politeness_delay_ms: u32,
    pub cross_domain_policy: Option<CrossDomainPolicy>,
    pub queues: std::collections::HashMap<PriorityBucket, Vec<PrioritizedUrl>>,
    pub status: ProcessingStatus,
    pub robots_disallowed: Option<Vec<String>>,
    pub rng_state: u32,
    pub processed_count: u64,
    pub error_count: u64,
}

impl DomainState {
    pub fn new(domain: String) -> Self {
        let mut queues = std::collections::HashMap::new();
        queues.insert(PriorityBucket::High, Vec::new());
        queues.insert(PriorityBucket::Medium, Vec::new());
        queues.insert(PriorityBucket::Low, Vec::new());
        Self {
            domain,
            politeness_delay_ms: 1000,
            cross_domain_policy: None,
            queues,
            status: ProcessingStatus::Inactive,
            robots_disallowed: None,
            rng_state: 12345,
            processed_count: 0,
            error_count: 0,
        }
    }

    pub fn validate_url(&self, url: &url::Url) -> bool {
        url.host_str().map(|d| d == self.domain).unwrap_or(false)
    }

    pub fn add_urls(&mut self, urls: Vec<PrioritizedUrl>) {
        for prioritized_url in urls {
            let mut existing_bucket_and_pos = None;

            for (&bucket, queue) in &self.queues {
                if let Some(pos) = queue.iter().position(|u| u.url == prioritized_url.url) {
                    existing_bucket_and_pos = Some((bucket, queue[pos].priority, pos));
                    break;
                }
            }

            let new_bucket = PriorityBucket::from_priority(prioritized_url.priority);

            if let Some((old_bucket, old_priority, pos)) = existing_bucket_and_pos {
                if prioritized_url.priority > old_priority {
                    if let Some(q) = self.queues.get_mut(&old_bucket) {
                        q.remove(pos);
                    }
                    if let Some(q) = self.queues.get_mut(&new_bucket) {
                        q.push(prioritized_url);
                    }
                }
            } else {
                if let Some(q) = self.queues.get_mut(&new_bucket) {
                    q.push(prioritized_url);
                }
            }
        }

        for queue in self.queues.values_mut() {
            queue.sort_by_key(|u| u.priority);
        }
    }

    pub fn add_urls_allowed_by_robots(&mut self, urls: Vec<PrioritizedUrl>) {
        let filtered_urls = urls
            .into_iter()
            .filter(|u| self.is_allowed_by_robots(&u.url))
            .collect();
        self.add_urls(filtered_urls);
    }

    pub fn has_pending(&self) -> bool {
        self.queues.values().any(|q| !q.is_empty())
    }

    fn next_random(&mut self) -> u32 {
        self.rng_state = self.rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        (self.rng_state / 65536) % 100
    }

    pub fn get_next_url(&mut self) -> Option<PrioritizedUrl> {
        if !self.has_pending() {
            return None;
        }

        let roll = self.next_random();

        let queue_order = if roll < 70 {
            vec![
                PriorityBucket::High,
                PriorityBucket::Medium,
                PriorityBucket::Low,
            ]
        } else if roll < 90 {
            vec![
                PriorityBucket::Medium,
                PriorityBucket::High,
                PriorityBucket::Low,
            ]
        } else {
            vec![
                PriorityBucket::Low,
                PriorityBucket::High,
                PriorityBucket::Medium,
            ]
        };

        for bucket in queue_order {
            if let Some(q) = self.queues.get_mut(&bucket)
                && !q.is_empty()
            {
                return q.pop();
            }
        }

        None
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
            !disallowed_rules.iter().any(|prefix| {
                if prefix == "/" {
                    true
                } else {
                    path.starts_with(prefix)
                }
            })
        } else {
            true
        }
    }
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum DomainCrawlerError {
    QueueFull { max_size: usize },
    InvalidUrlForDomain { url: String, domain: String },
    ConfigurationError { message: String },
    FetcherFailed { message: String },
}

#[agent_definition(mount = "/domains/{domain_name}", snapshotting = "every(10)")]
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

    // Adjust cross-domain policy dynamically via REST.
    #[endpoint(post = "/config/cross-domain-policy")]
    async fn set_cross_domain_policy(
        &mut self,
        policy: CrossDomainPolicy,
    ) -> Result<(), DomainCrawlerError>;

    // Remove custom cross-domain policy and fallback to default configuration.
    #[endpoint(delete = "/config/cross-domain-policy")]
    async fn remove_cross_domain_policy(&mut self) -> Result<(), DomainCrawlerError>;

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
        if let Some(invalid_url) = urls.iter().find(|u| !self.state.validate_url(&u.url)) {
            Err(DomainCrawlerError::InvalidUrlForDomain {
                url: invalid_url.url.to_string(),
                domain: self.state.domain.clone(),
            })
        } else {
            // Add URLs to the queue (handles duplicates and sorting)
            self.state.add_urls(urls);

            // Start processing if inactive and we have items
            if self.state.is_inactive() && self.state.has_pending() {
                self.schedule_next_step();
            }

            Ok(())
        }
    }

    async fn get_state(&self) -> Result<DomainState, DomainCrawlerError> {
        Ok(self.state.clone())
    }

    async fn set_delay(&mut self, delay_ms: u32) -> Result<(), DomainCrawlerError> {
        self.state.set_delay(delay_ms);
        Ok(())
    }

    async fn set_cross_domain_policy(
        &mut self,
        policy: CrossDomainPolicy,
    ) -> Result<(), DomainCrawlerError> {
        self.state.cross_domain_policy = Some(policy);
        Ok(())
    }

    async fn remove_cross_domain_policy(&mut self) -> Result<(), DomainCrawlerError> {
        self.state.cross_domain_policy = None;
        Ok(())
    }

    async fn process_next(&mut self) -> Result<(), DomainCrawlerError> {
        if self.state.has_pending() {
            // 1. Ensure robots.txt rules are fetched and cached
            if self.state.robots_disallowed.is_none() {
                log::info!("Fetching robots.txt for domain: {}", self.state.domain);
                let (disallowed, delay_ms) = fetch_and_parse_robots(&self.state.domain).await;
                self.state.robots_disallowed = Some(disallowed);
                if let Some(delay) = delay_ms {
                    log::info!("Setting politeness delay to {}ms from Crawl-delay", delay);
                    self.state.set_delay(delay);
                }
            }

            if let Some(target) = self.state.get_next_url() {
                // 2. Check compliance with robots.txt rules
                if !self.state.is_allowed_by_robots(&target.url) {
                    log::info!("URL disallowed by robots.txt, skipping: {}", target.url);
                    self.schedule_next_step();
                } else {
                    self.state.set_status(ProcessingStatus::Processing);

                    // Fetch using the FetcherAgent worker
                    let fetcher = FetcherAgentClient::new_phantom();
                    let fetch_result = fetcher.fetch_and_parse(target.url.clone()).await;

                    match fetch_result {
                        Ok(result) => {
                            self.state.processed_count += 1;
                            self.process_extracted_links(result.extracted_links).await;
                        }
                        Err(e) => {
                            log::error!("Failed to fetch URL {}: {:?}", target.url, e);
                            self.state.error_count += 1;
                        }
                    }

                    self.schedule_next_step();
                }
            } else {
                self.state.set_status(ProcessingStatus::Inactive);
            }
        } else {
            self.state.set_status(ProcessingStatus::Inactive);
        }
        Ok(())
    }

    async fn load_snapshot(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        let state: DomainState = serde_json::from_slice(&bytes)
            .map_err(|e| format!("Failed to deserialize snapshot: {:?}", e))?;
        self.state = state;
        Ok(())
    }

    async fn save_snapshot(&self) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(&self.state)
            .map_err(|e| format!("Failed to serialize snapshot: {:?}", e))?;
        Ok(bytes)
    }
}

fn is_subdomain(sub: &str, parent: &str) -> bool {
    sub == parent || sub.ends_with(&format!(".{}", parent))
}

impl DomainCrawlerAgentImpl {
    async fn process_extracted_links(&mut self, extracted_links: Vec<url::Url>) {
        let config = self.config.get();
        let db_cfg = config.db;
        let max_url_len = config.url_processing.max_url_length.get();
        let boost_words = config.url_processing.boost_words.get();

        let current_policy = self.state.cross_domain_policy.clone().unwrap_or_else(|| {
            let config_policy_str = config.url_processing.cross_domain_policy.get();
            config_policy_str.parse().unwrap_or(CrossDomainPolicy::None)
        });

        // 1. Filter out URLs exceeding max length or domain constraints first
        let mut candidate_urls = Vec::new();
        for link_url in extracted_links {
            let link_str = link_url.to_string();
            if link_str.len() as u32 > max_url_len {
                continue;
            }
            if let Some(domain) = link_url.host_str() {
                let is_allowed = match current_policy {
                    CrossDomainPolicy::None => domain == self.state.domain,
                    CrossDomainPolicy::SubdomainsOnly => is_subdomain(domain, &self.state.domain),
                    CrossDomainPolicy::Any => true,
                };
                if is_allowed {
                    candidate_urls.push(link_url);
                }
            }
        }

        // 2. Query DB to filter out already crawled URLs
        let cache_ttl = config.url_processing.cache_ttl_seconds.get();
        let uncrawled_urls = match filter_uncrawled_urls(db_cfg, candidate_urls, cache_ttl).await {
            Ok(filtered) => filtered,
            Err(e) => {
                log::error!("Failed to filter uncrawled URLs: {:?}", e);
                return;
            }
        };

        // 3. Group and prioritize URLs by domain
        let grouped_by_domain =
            crate::common::group_prioritized_urls_by_domain(uncrawled_urls, |u| {
                let link_str = u.to_string();
                let priority = calculate_priority(&link_str, &boost_words);
                PrioritizedUrl { url: u, priority }
            });

        for (domain, prioritized_urls) in grouped_by_domain {
            if domain == self.state.domain {
                self.state.add_urls_allowed_by_robots(prioritized_urls);
            } else {
                let mut client = DomainCrawlerAgentClient::get(domain);
                client.trigger_enqueue(prioritized_urls);
            }
        }
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

async fn filter_uncrawled_urls(
    db_cfg: PostgresDbConfig,
    urls: Vec<url::Url>,
    cache_ttl_seconds: Option<u64>,
) -> Result<Vec<url::Url>, DomainCrawlerError> {
    if urls.is_empty() {
        Ok(urls)
    } else if let Some(0) = cache_ttl_seconds {
        // Force re-crawl everything: no database check or transaction needed
        Ok(urls)
    } else {
        let db_helper =
            DatabaseHelper::from(db_cfg).map_err(|e| DomainCrawlerError::ConfigurationError {
                message: format!("Failed to connect to database: {:?}", e),
            })?;

        let url_strs: Vec<String> = urls.iter().map(|u| u.to_string()).collect();

        let crawled_urls: std::collections::HashSet<String> = db_helper
            .transactional(|tx| {
                match cache_ttl_seconds {
                    Some(ttl) => {
                        let sql = "SELECT url FROM page_contents WHERE url = ANY($1) AND saved_at > CURRENT_TIMESTAMP - ($2 || ' second')::INTERVAL";
                        let ttl_i64 = ttl as i64;
                        let res = tx.query(sql, crate::encode_params!(&url_strs, &ttl_i64))?;
                        use crate::common_lib::database::decode::DbResultDecoder;
                        let rows = Single::<String>::decode_result(res)?;
                        Ok(rows.into_iter().map(|s| s.0).collect())
                    }
                    None => {
                        let sql = "SELECT url FROM page_contents WHERE url = ANY($1)";
                        let res = tx.query(sql, crate::encode_params!(&url_strs))?;
                        use crate::common_lib::database::decode::DbResultDecoder;
                        let rows = Single::<String>::decode_result(res)?;
                        Ok(rows.into_iter().map(|s| s.0).collect())
                    }
                }
            })
            .map_err(|e| DomainCrawlerError::FetcherFailed {
                message: format!("Failed to query crawled URLs: {:?}", e),
            })?;

        Ok(urls
            .into_iter()
            .filter(|u| !crawled_urls.contains(&u.to_string()))
            .collect())
    }
}

async fn fetch_and_parse_robots(domain: &str) -> (Vec<String>, Option<u32>) {
    let robots_url = format!("https://{}/robots.txt", domain);
    let client = golem_wasi_http::Client::new();
    match client.get(&robots_url).send() {
        Ok(response) if response.status().as_u16() == 200 => match response.text() {
            Ok(body) => parse_robots_txt(&body),
            Err(_) => (Vec::new(), None),
        },
        Ok(_) => (Vec::new(), None),
        Err(e) => {
            log::warn!("Failed to fetch robots.txt for {}: {:?}", domain, e);
            (Vec::new(), None)
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
            } else if key == "crawl-delay"
                && let Ok(secs) = val.parse::<f64>()
            {
                crawl_delay = Some((secs * 1000.0) as u32);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_subdomain() {
        assert!(is_subdomain("golem.cloud", "golem.cloud"));
        assert!(is_subdomain("learn.golem.cloud", "golem.cloud"));
        assert!(is_subdomain("abc.xyz.golem.cloud", "golem.cloud"));
        assert!(!is_subdomain("newdomain.com", "golem.cloud"));
        assert!(!is_subdomain("othergolem.cloud", "golem.cloud"));
        assert!(!is_subdomain("golem.cloud.net", "golem.cloud"));
    }

    #[test]
    fn test_domain_state_new() {
        let state = DomainState::new("example.com".to_string());
        assert_eq!(state.domain, "example.com");
        assert_eq!(state.get_delay(), 1000);
        assert!(state.is_inactive());
        assert!(!state.has_pending());
        assert_eq!(state.processed_count, 0);
        assert_eq!(state.error_count, 0);
        assert_eq!(state.cross_domain_policy, None);
    }

    #[test]
    fn test_validate_url() {
        let state = DomainState::new("example.com".to_string());
        assert!(state.validate_url(&url::Url::parse("https://example.com/about").unwrap()));
        assert!(!state.validate_url(&url::Url::parse("https://other.com/about").unwrap()));
    }

    #[test]
    fn test_add_and_get_urls() {
        let mut state = DomainState::new("example.com".to_string());
        let url1 = PrioritizedUrl {
            url: url::Url::parse("https://example.com/1").unwrap(),
            priority: 5,
        };
        let url2 = PrioritizedUrl {
            url: url::Url::parse("https://example.com/2").unwrap(),
            priority: 15,
        };
        state.add_urls(vec![url1.clone(), url2.clone()]);
        assert!(state.has_pending());

        let popped1 = state.get_next_url().unwrap();
        assert_eq!(popped1.priority, 15);
        assert_eq!(popped1.url.as_str(), "https://example.com/2");

        let popped2 = state.get_next_url().unwrap();
        assert_eq!(popped2.priority, 5);
        assert_eq!(popped2.url.as_str(), "https://example.com/1");

        assert!(!state.has_pending());
    }

    #[test]
    fn test_robots_exclusion() {
        let mut state = DomainState::new("example.com".to_string());
        state.robots_disallowed = Some(vec!["/private".to_string(), "/temp/".to_string()]);

        let allowed = url::Url::parse("https://example.com/public").unwrap();
        let disallowed1 = url::Url::parse("https://example.com/private/data").unwrap();
        let disallowed2 = url::Url::parse("https://example.com/temp/file.txt").unwrap();

        assert!(state.is_allowed_by_robots(&allowed));
        assert!(!state.is_allowed_by_robots(&disallowed1));
        assert!(!state.is_allowed_by_robots(&disallowed2));
    }

    #[test]
    fn test_add_urls_allowed_by_robots() {
        let mut state = DomainState::new("example.com".to_string());
        state.robots_disallowed = Some(vec!["/private".to_string()]);

        let url1 = PrioritizedUrl {
            url: url::Url::parse("https://example.com/public").unwrap(),
            priority: 10,
        };
        let url2 = PrioritizedUrl {
            url: url::Url::parse("https://example.com/private/data").unwrap(),
            priority: 10,
        };

        state.add_urls_allowed_by_robots(vec![url1, url2]);

        assert!(state.has_pending());
        let next = state.get_next_url().unwrap();
        assert_eq!(next.url.as_str(), "https://example.com/public");
        assert!(!state.has_pending());
    }

    #[test]
    fn test_duplicate_url_priority_promotion() {
        let mut state = DomainState::new("example.com".to_string());
        let url_shared = url::Url::parse("https://example.com/item").unwrap();

        // Add as low priority (priority 2)
        state.add_urls(vec![PrioritizedUrl {
            url: url_shared.clone(),
            priority: 2,
        }]);
        assert_eq!(state.queues.get(&PriorityBucket::Low).unwrap().len(), 1);
        assert_eq!(state.queues.get(&PriorityBucket::High).unwrap().len(), 0);

        // Re-enqueue with high priority (priority 12)
        state.add_urls(vec![PrioritizedUrl {
            url: url_shared.clone(),
            priority: 12,
        }]);
        // Should migrate to high_queue and update priority
        assert_eq!(state.queues.get(&PriorityBucket::Low).unwrap().len(), 0);
        assert_eq!(state.queues.get(&PriorityBucket::High).unwrap().len(), 1);
        assert_eq!(
            state.queues.get(&PriorityBucket::High).unwrap()[0].priority,
            12
        );
    }

    #[test]
    fn test_lottery_weighted_selection() {
        let mut state = DomainState::new("example.com".to_string());
        // Put one URL in each queue
        state.add_urls(vec![
            PrioritizedUrl {
                url: url::Url::parse("https://example.com/low").unwrap(),
                priority: 2,
            },
            PrioritizedUrl {
                url: url::Url::parse("https://example.com/med").unwrap(),
                priority: 7,
            },
            PrioritizedUrl {
                url: url::Url::parse("https://example.com/high").unwrap(),
                priority: 12,
            },
        ]);

        // Keep polling and verify we eventually pop all three
        let mut popped = Vec::new();
        while let Some(item) = state.get_next_url() {
            popped.push(item.url.as_str().to_string());
        }

        assert_eq!(popped.len(), 3);
        assert!(popped.contains(&"https://example.com/low".to_string()));
        assert!(popped.contains(&"https://example.com/med".to_string()));
        assert!(popped.contains(&"https://example.com/high".to_string()));
    }
}
