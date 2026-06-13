use crate::common_lib::database::{DatabaseHelper, PostgresDbConfig};
use golem_rust::agentic::Config;
use golem_rust::{ConfigSchema, Schema, agent_definition, agent_implementation, endpoint};
use serde::{Deserialize, Serialize};

#[derive(ConfigSchema)]
pub struct SearchConfig {
    #[config_schema(nested)]
    pub db: PostgresDbConfig,
}

#[derive(Clone, Debug, Schema, Serialize, Deserialize)]
pub struct SearchResultPage {
    pub url: String,
    pub title: Option<String>,
    pub domain: String,
    pub http_status: i32,
    pub saved_at: String,
}

crate::db_row_decoder!(SearchResultPage {
    url,
    title,
    domain,
    http_status,
    saved_at,
});

#[agent_definition(ephemeral, mount = "/search")]
pub trait SearchAgent {
    fn new(#[agent_config] config: Config<SearchConfig>) -> Self;

    #[endpoint(get = "/?query={query}")]
    async fn search(&self, query: String) -> Result<Vec<SearchResultPage>, String>;
}

pub struct SearchAgentImpl {
    config: Config<SearchConfig>,
}

#[agent_implementation]
impl SearchAgent for SearchAgentImpl {
    fn new(#[agent_config] config: Config<SearchConfig>) -> Self {
        Self { config }
    }

    async fn search(&self, query: String) -> Result<Vec<SearchResultPage>, String> {
        let db_cfg = self.config.get().db;
        let db_helper = DatabaseHelper::from(db_cfg)
            .map_err(|e| format!("Failed to connect to database: {:?}", e))?;

        // Query executing Full-Text Search
        let results = db_helper.transactional(|tx| {
            let sql = "SELECT url, domain, title, http_status, TO_CHAR(saved_at, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS saved_at \
                       FROM page_contents \
                       WHERE to_tsvector('english', coalesce(title, '') || ' ' || coalesce(extracted_text, '')) \
                             @@ plainto_tsquery('english', $1) \
                       ORDER BY ts_rank(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(extracted_text, '')), plainto_tsquery('english', $1)) DESC \
                       LIMIT 50";

            let res = tx.query(sql, crate::encode_params!(&query))?;

            use crate::common_lib::database::decode::DbResultDecoder;
            SearchResultPage::decode_result(res)
        }).map_err(|e| format!("Database query failed: {:?}", e))?;

        Ok(results)
    }
}
