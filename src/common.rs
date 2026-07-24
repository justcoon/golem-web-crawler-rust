use golem_rust::agentic::Secret;
use golem_rust::{ConfigSchema, Schema};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct UrlProcessingConfig {
    #[config_schema(secret)]
    pub boost_words: Secret<Vec<String>>,
    #[config_schema(secret)]
    pub max_url_length: Secret<u32>,
    #[config_schema(secret)]
    pub cross_domain_policy: Secret<String>,
    #[config_schema(secret)]
    pub normalize_prefixes: Secret<Vec<String>>,
    #[config_schema(secret)]
    pub cache_ttl_seconds: Secret<Option<u64>>,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct PrioritizedUrl {
    pub url: url::Url,
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Schema, Serialize, Deserialize)]
pub enum FilterType {
    DomainBlacklist,
    UrlRegex,
    Keyword,
    Extension,
}

impl std::str::FromStr for FilterType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "domain_blacklist" => Ok(FilterType::DomainBlacklist),
            "url_regex" => Ok(FilterType::UrlRegex),
            "keyword" => Ok(FilterType::Keyword),
            "extension" => Ok(FilterType::Extension),
            _ => Err(format!("Unknown filter type: {}", s)),
        }
    }
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FilterType::DomainBlacklist => "domain_blacklist",
            FilterType::UrlRegex => "url_regex",
            FilterType::Keyword => "keyword",
            FilterType::Extension => "extension",
        };
        write!(f, "{}", s)
    }
}

impl crate::common_lib::database::decode::DbValueDecoder for FilterType {
    fn decode(value: &crate::common_lib::database::PostgresDbValue) -> anyhow::Result<Self> {
        let s = String::decode(value)?;
        s.parse().map_err(|e| anyhow::anyhow!("{}", e))
    }
}

impl crate::common_lib::database::encode::DbValueEncoder for FilterType {
    fn encode(self) -> crate::common_lib::database::PostgresDbValue {
        crate::common_lib::database::PostgresDbValue::Text(self.to_string())
    }
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct LinkFilter {
    pub id: i32,
    pub pattern: String,
    pub filter_type: FilterType,
    pub is_active: bool,
    pub created_at: String,
}

crate::db_row_decoder!(LinkFilter {
    id,
    pattern,
    filter_type,
    is_active,
    created_at,
});

pub fn normalize_domain(domain: &str, normalize_prefixes: &[String]) -> String {
    let domain_lower = domain.to_lowercase();
    for prefix in normalize_prefixes {
        let prefix_dot = format!("{}.", prefix.to_lowercase());
        if domain_lower.starts_with(&prefix_dot) {
            return domain_lower[prefix_dot.len()..].to_string();
        }
    }
    domain_lower
}

pub fn normalize_url_domain(url: &url::Url, normalize_prefixes: &[String]) -> url::Url {
    if let Some(host) = url.host_str() {
        let normalized = normalize_domain(host, normalize_prefixes);
        if normalized != host {
            let mut u = url.clone();
            let _ = u.set_host(Some(&normalized));
            return u;
        }
    }
    url.clone()
}

pub fn group_urls_by_domain(
    urls: Vec<url::Url>,
) -> std::collections::HashMap<String, Vec<url::Url>> {
    let mut grouped = std::collections::HashMap::new();
    for url in urls {
        if let Some(host) = url.host_str() {
            grouped
                .entry(host.to_string())
                .or_insert_with(Vec::new)
                .push(url);
        }
    }
    grouped
}

pub fn group_prioritized_urls_by_domain<F>(
    urls: Vec<url::Url>,
    prioritize: F,
) -> std::collections::HashMap<String, Vec<PrioritizedUrl>>
where
    F: Fn(url::Url) -> PrioritizedUrl,
{
    let grouped_urls = group_urls_by_domain(urls);
    let mut grouped = std::collections::HashMap::new();
    for (domain, urls) in grouped_urls {
        let prioritized = urls.into_iter().map(&prioritize).collect();
        grouped.insert(domain, prioritized);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_domain() {
        let prefixes = vec!["www".to_string(), "m".to_string()];
        let url = url::Url::parse("https://www.golem.cloud/docs").unwrap();
        let normalized = normalize_url_domain(&url, &prefixes);
        assert_eq!(normalized.as_str(), "https://golem.cloud/docs");
    }

    #[test]
    fn test_group_urls_by_domain() {
        let urls = vec![
            url::Url::parse("https://golem.cloud/docs").unwrap(),
            url::Url::parse("https://golem.cloud/blog").unwrap(),
            url::Url::parse("https://example.com/index").unwrap(),
            url::Url::parse("https://example.com/about").unwrap(),
        ];
        let grouped = group_urls_by_domain(urls);
        assert_eq!(grouped.get("golem.cloud").unwrap().len(), 2);
        assert_eq!(grouped.get("example.com").unwrap().len(), 2);
    }

    #[test]
    fn test_group_prioritized_urls_by_domain() {
        let urls = vec![
            url::Url::parse("https://golem.cloud/docs").unwrap(),
            url::Url::parse("https://example.com/about").unwrap(),
        ];
        let grouped = group_prioritized_urls_by_domain(urls, |u| {
            let priority = if u.path().contains("docs") { 20 } else { 5 };
            PrioritizedUrl { url: u, priority }
        });

        let golem_urls = grouped.get("golem.cloud").unwrap();
        assert_eq!(golem_urls.len(), 1);
        assert_eq!(golem_urls[0].priority, 20);

        let example_urls = grouped.get("example.com").unwrap();
        assert_eq!(example_urls.len(), 1);
        assert_eq!(example_urls[0].priority, 5);
    }
}
