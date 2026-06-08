// src/fetcher.rs
use crate::common::PrioritizedUrl;
use golem_rust::{Schema, agent_definition};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct FetchResult {
    pub url: String,
    pub title: String,
    // Links extracted from the page along with priority information
    pub extracted_links: Vec<PrioritizedUrl>,
    pub status: u16,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub enum FetcherError {
    InvalidUrl {
        url: String,
        reason: String,
    },
    RobotsDisallowed {
        url: String,
    },
    HttpFetchFailed {
        url: String,
        status_code: u16,
        message: String,
    },
    PostgresWriteFailed {
        message: String,
    },
}

#[agent_definition(ephemeral)]
pub trait FetcherAgent {
    // Constructor identifies the worker instance.
    fn new(worker_id: String) -> Self;

    // Fetch the page using golem-wasi-http and persist results to PostgreSQL.
    async fn fetch_and_parse(&self, url: String) -> Result<FetchResult, FetcherError>;
}
