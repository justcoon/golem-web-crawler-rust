-- V1__Create_Crawler_Tables.sql
-- Database migration script to initialize the Golem web crawler schema.

-- Create the page_contents table to hold raw and extracted crawl output
CREATE TABLE page_contents (
    url TEXT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    title VARCHAR(512),
    http_status INT,
    raw_html TEXT,
    extracted_text TEXT,
    saved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index to optimize domain queries and listing
CREATE INDEX idx_page_contents_domain ON page_contents(domain);

-- Index to optimize Full-Text Search (FTS) on title and extracted text
CREATE INDEX idx_page_contents_fts ON page_contents 
USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(extracted_text, '')));
