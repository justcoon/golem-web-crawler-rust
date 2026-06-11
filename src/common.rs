// src/common.rs
use golem_rust::Schema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct PrioritizedUrl {
    pub url: url::Url,
    pub priority: i32,
}

pub fn get_domain_from_url(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(String::from))
}
