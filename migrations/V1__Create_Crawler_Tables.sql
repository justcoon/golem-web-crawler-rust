-- V1__Create_Crawler_Tables.sql
-- Database migration script to initialize the Golem web crawler schema.

-- Create the page_contents table to hold raw and extracted crawl output
CREATE TABLE page_contents (
    url TEXT PRIMARY KEY,
    title VARCHAR(512),
    http_status INT,
    raw_html TEXT,
    extracted_text TEXT,
    saved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
