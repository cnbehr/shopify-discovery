-- Discovery Service - Owned Tables
-- These are the ONLY tables this service has write access to.
-- See SCHEMA.md for full column-level contract details.

-- Master store registry with normalized domains
CREATE TABLE IF NOT EXISTS `shopify_intelligence.stores` (
  store_id STRING NOT NULL,  -- SHA256 hash of normalized domain
  domain STRING NOT NULL,    -- Normalized domain (no www, lowercase)
  original_domain STRING,    -- Original domain as discovered

  -- Discovery metadata
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP,
  source_crawl STRING,       -- e.g., 'CC-MAIN-2025-05'
  discovery_method STRING,   -- 'common_crawl', 'manual', 'referral'

  -- Store status
  is_active BOOL DEFAULT TRUE,
  last_http_status INT64,
  last_checked_at TIMESTAMP,
  consecutive_failures INT64 DEFAULT 0,

  -- Basic classification
  estimated_size STRING,     -- 'small', 'medium', 'large', 'enterprise'
  sophistication_score FLOAT64,

  -- Deep enrichment tracking
  last_deep_enriched_at TIMESTAMP,

  -- Timestamps
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY domain, is_active;


-- Store discovery staging (populated by WAT scanner, reconciled into stores)
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_discovery_staging` (
  domain STRING NOT NULL,
  crawl_id STRING NOT NULL,
  discovery_method STRING NOT NULL,
  confidence FLOAT64 NOT NULL,
  discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY domain, crawl_id;


-- Enrichment queue (Discovery INSERTs new entries; Enrichment UPDATEs status)
-- SHARED OWNERSHIP: Discovery inserts, Enrichment updates.
-- Column changes require coordination with Enrichment service.
CREATE TABLE IF NOT EXISTS `shopify_intelligence.enrichment_queue` (
  queue_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,

  priority INT64 DEFAULT 0,
  enrichment_tier STRING,
  snapshot_type STRING,
  activity_only BOOL DEFAULT FALSE,
  reason STRING,

  status STRING DEFAULT 'pending',
  attempts INT64 DEFAULT 0,
  max_attempts INT64 DEFAULT 3,

  locked_by STRING,
  locked_at TIMESTAMP,

  scheduled_at TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,

  last_error STRING,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY status, priority;
