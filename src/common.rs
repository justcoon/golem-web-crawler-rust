// src/common.rs
use golem_rust::Schema;
use serde::{Deserialize, Serialize};

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
