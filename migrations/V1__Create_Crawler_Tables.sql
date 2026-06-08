-- V1__Create_Crawler_Tables.sql
-- Database migration script to initialize the Golem web crawler schema.

-- 1. Create the crawl_frontier table to manage URL queues and state
CREATE TABLE crawl_frontier (
    url_hash VARCHAR(64) PRIMARY KEY, -- SHA-256 hash of URL for O(1) deduplication
    url TEXT NOT NULL,
    domain VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    priority INT NOT NULL DEFAULT 0,
    depth INT NOT NULL DEFAULT 0,
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    crawled_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT
);

-- Index for DomainCrawlerAgent to load high-priority pending URLs quickly
CREATE INDEX idx_frontier_domain_status_priority 
ON crawl_frontier(domain, status, priority DESC, discovered_at ASC);

-- 2. Create the page_contents table to hold raw and extracted crawl output
CREATE TABLE page_contents (
    url_hash VARCHAR(64) PRIMARY KEY REFERENCES crawl_frontier(url_hash) ON DELETE CASCADE,
    title VARCHAR(512),
    http_status INT,
    raw_html TEXT,
    extracted_text TEXT,
    saved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
