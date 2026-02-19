-- Shopify Intelligence Platform - BigQuery Schema v2
-- Dataset: shopify_intelligence

-- ============================================================================
-- CORE TABLES
-- ============================================================================

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
  sophistication_score FLOAT64,  -- Calculated maturity score 0-100

  -- Timestamps
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY domain, is_active;

-- Time-series snapshots (partitioned by date)
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_snapshots` (
  snapshot_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,
  snapshot_type STRING NOT NULL,  -- 'weekly', 'monthly', 'daily'

  -- HTTP response data
  http_status INT64,
  response_time_ms INT64,
  redirect_chain ARRAY<STRING>,
  final_url STRING,

  -- Page metadata
  page_title STRING,
  meta_description STRING,
  canonical_url STRING,

  -- Detected tech (summary counts)
  pixel_count INT64,
  app_count INT64,

  -- Revenue signals
  product_count INT64,
  collection_count INT64,
  price_min FLOAT64,
  price_max FLOAT64,
  price_avg FLOAT64,

  -- Stock signals
  in_stock_products INT64,
  out_of_stock_products INT64,

  -- Raw data references
  html_hash STRING,          -- Hash of page HTML for change detection
  screenshot_gcs_path STRING,
  screenshot_mobile_gcs_path STRING,

  -- Processing metadata
  enrichment_tier STRING,    -- 'http', 'browser'
  processing_duration_ms INT64,
  error_message STRING,

  -- Deep enrichment fields
  deep_enrichment BOOL,
  pages_fetched ARRAY<STRING>,
  total_bytes_fetched INT64,
  theme_name STRING,
  theme_store_id INT64,
  is_custom_theme BOOL,
  is_shopify_plus BOOL,
  shopify_plus_signals ARRAY<STRING>,
  page_builder STRING,
  sitemap_product_count INT64,
  sitemap_collection_count INT64,
  sitemap_page_count INT64,
  sitemap_blog_count INT64,
  store_currency STRING,
  store_country STRING,
  refund_window_days INT64,
  shopify_section_count INT64,
  app_block_count INT64,
  app_block_names ARRAY<STRING>,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain, snapshot_type;

-- Materialized view of current state (latest snapshot per store)
CREATE OR REPLACE VIEW `shopify_intelligence.store_latest` AS
SELECT s.*,
  snap.snapshot_date AS last_snapshot_date,
  snap.http_status AS current_http_status,
  snap.pixel_count,
  snap.app_count,
  snap.product_count,
  snap.price_min,
  snap.price_max,
  snap.price_avg,
  snap.in_stock_products,
  snap.out_of_stock_products,
  snap.screenshot_gcs_path
FROM `shopify_intelligence.stores` s
LEFT JOIN (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY snapshot_date DESC) as rn
  FROM `shopify_intelligence.store_snapshots`
) snap ON s.store_id = snap.store_id AND snap.rn = 1;

-- ============================================================================
-- FETCH COST METRICS
-- ============================================================================

-- Per-store fetch metrics (bytes/proxy usage) for tuning byte budgets and proxy fallback behavior.
CREATE TABLE IF NOT EXISTS `shopify_intelligence.enrichment_fetch_metrics` (
  metric_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,
  snapshot_type STRING NOT NULL,
  tier STRING NOT NULL, -- 'http'

  homepage_bytes INT64,
  used_proxy BOOL,
  attempts INT64,
  http_status INT64,
  error_message STRING,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain, snapshot_type;


-- ============================================================================
-- DETECTION TABLES
-- ============================================================================

-- Unified detections table — replaces pixel_detections, app_detections, technology_detections
-- All tool/pixel/tech detections use a single schema with a type discriminator.
CREATE TABLE IF NOT EXISTS `shopify_intelligence.detections` (
  detection_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,

  -- Type discriminator: 'pixel', 'app', or 'technology'
  detection_type STRING NOT NULL,

  -- Unified entity identification
  entity_id STRING NOT NULL,     -- tool_id (was pixel_id / app_id / tech_name)
  entity_name STRING NOT NULL,   -- display name (was pixel_name / app_name / tech_name)
  entity_category STRING,        -- category (was pixel_category / app_category / tech_category)
  entity_vendor STRING,          -- vendor (from app_vendor)

  -- Detection details
  detection_method STRING,       -- 'script_src', 'inline_script', 'cookie', 'window_object'
  detection_pattern STRING,      -- The pattern that matched
  matched_value STRING,          -- The actual matched content

  -- Optional fields
  detected_version STRING,       -- Version info if detectable
  entity_identifier STRING,      -- Extracted identifier (e.g., Pixel ID, GA4 Measurement ID)

  confidence_score FLOAT64,      -- 0-1 confidence in detection

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain, detection_type, entity_category;


-- ============================================================================
-- BUSINESS INTELLIGENCE TABLES
-- ============================================================================

-- Best sellers, pricing, inventory signals (summary level)
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_products` (
  record_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,

  -- Product counts
  total_products INT64,
  total_variants INT64,
  total_collections INT64,

  -- Pricing stats
  price_min FLOAT64,
  price_max FLOAT64,
  price_avg FLOAT64,
  price_median FLOAT64,
  currency STRING,

  -- Stock stats
  in_stock_count INT64,
  out_of_stock_count INT64,
  low_stock_count INT64,       -- If detectable

  -- Best sellers (top 10)
  best_sellers ARRAY<STRUCT<
    product_id STRING,
    handle STRING,
    title STRING,
    price FLOAT64,
    position INT64
  >>,

  -- New arrivals
  new_arrivals_count INT64,    -- Products created in last 30 days

  -- Price distribution
  price_buckets ARRAY<STRUCT<
    bucket_min FLOAT64,
    bucket_max FLOAT64,
    product_count INT64
  >>,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain;

-- Brand, social, and contact info (merged from store_brand_info + store_contact_info)
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_business_info` (
  record_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,

  -- Brand basics
  store_name STRING,
  brand_description STRING,
  tagline STRING,

  -- Screenshots
  screenshot_desktop_url STRING,
  screenshot_mobile_url STRING,
  logo_url STRING,
  favicon_url STRING,

  -- Social links
  instagram_url STRING,
  instagram_handle STRING,
  tiktok_url STRING,
  tiktok_handle STRING,
  facebook_url STRING,
  twitter_url STRING,
  youtube_url STRING,
  pinterest_url STRING,
  linkedin_url STRING,

  -- Other links
  blog_url STRING,
  about_page_url STRING,

  -- Contact details
  email_addresses ARRAY<STRING>,
  phone_numbers ARRAY<STRING>,

  -- Physical address
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,
  full_address STRING,

  -- Business info
  business_name STRING,
  vat_number STRING,

  -- Source
  contact_source STRING,       -- 'footer', 'contact_page', 'about_page'

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain;

-- NOTE: store_revenue_signals has been removed (was never populated by any worker).
-- NOTE: store_shipping has been removed (was never populated by any worker).


-- ============================================================================
-- PRODUCT INTELLIGENCE TABLES (Phase 6)
-- ============================================================================

-- Detailed product data for analysis
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_products_full` (
  record_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,
  snapshot_date DATE NOT NULL,

  -- Product identifiers
  product_id STRING NOT NULL,
  handle STRING,

  -- Product details
  title STRING,
  product_type STRING,         -- Hierarchical type from store
  vendor STRING,
  tags ARRAY<STRING>,

  -- Description
  description_html STRING,
  description_text STRING,     -- Cleaned plain text

  -- Images
  image_urls ARRAY<STRING>,
  featured_image_url STRING,

  -- Pricing
  price_min FLOAT64,
  price_max FLOAT64,
  compare_at_price_min FLOAT64,
  compare_at_price_max FLOAT64,
  currency STRING,

  -- Variants
  total_variants INT64,
  available_variants INT64,    -- Count of in-stock variants
  variant_options ARRAY<STRING>,  -- e.g., ['Size', 'Color']

  -- Availability
  is_available BOOL,           -- Any variant in stock

  -- Timestamps
  product_created_at TIMESTAMP,
  product_updated_at TIMESTAMP,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY domain, product_type;

-- Product images for similarity detection
CREATE TABLE IF NOT EXISTS `shopify_intelligence.product_images` (
  record_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,

  product_id STRING NOT NULL,
  product_handle STRING,
  product_title STRING,

  -- Image details
  image_url STRING NOT NULL,
  image_position INT64,        -- Order in product gallery

  -- Image metadata
  width INT64,
  height INT64,
  alt_text STRING,

  -- Perceptual hashes for similarity
  phash STRING,                -- Perceptual hash (64-bit hex)
  dhash STRING,                -- Difference hash
  ahash STRING,                -- Average hash

  -- For clustering similar images
  phash_bucket STRING,         -- First 16 bits for quick filtering

  snapshot_date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY snapshot_date
CLUSTER BY phash_bucket, domain;

-- NOTE: store_shipping table has been removed (was never populated by any worker).
-- Shipping data (offers_free_shipping, free_shipping_threshold) is captured
-- directly in store_snapshots via deep enrichment.


-- ============================================================================
-- LOOKUP TABLES
-- ============================================================================

-- Pixel catalog with detection patterns
CREATE TABLE IF NOT EXISTS `shopify_intelligence.pixel_definitions` (
  pixel_id STRING NOT NULL,
  pixel_name STRING NOT NULL,
  display_name STRING NOT NULL,
  vendor STRING,
  category STRING NOT NULL,      -- 'advertising', 'analytics', 'heatmap', 'ab_testing'

  -- Detection patterns (JSON arrays)
  script_patterns ARRAY<STRING>,       -- URL patterns to match in script src
  inline_patterns ARRAY<STRING>,       -- Regex patterns for inline scripts
  cookie_patterns ARRAY<STRING>,       -- Cookie name patterns
  window_patterns ARRAY<STRING>,       -- window.X object patterns
  dom_patterns ARRAY<STRING>,          -- CSS selectors for widgets
  header_patterns ARRAY<STRING>,       -- HTTP response header patterns

  -- Identifier extraction
  identifier_pattern STRING,           -- Regex to extract pixel ID
  identifier_name STRING,              -- e.g., 'Pixel ID', 'Measurement ID'

  -- Metadata
  website_url STRING,
  documentation_url STRING,
  description STRING,

  is_active BOOL DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- App catalog with detection patterns
CREATE TABLE IF NOT EXISTS `shopify_intelligence.app_definitions` (
  app_id STRING NOT NULL,
  app_name STRING NOT NULL,
  display_name STRING NOT NULL,
  vendor STRING,
  category STRING NOT NULL,
  subcategory STRING,

  -- Detection patterns
  script_patterns ARRAY<STRING>,
  inline_patterns ARRAY<STRING>,
  cookie_patterns ARRAY<STRING>,
  window_patterns ARRAY<STRING>,
  dom_patterns ARRAY<STRING>,
  header_patterns ARRAY<STRING>,

  -- Shopify-specific
  shopify_app_id STRING,
  shopify_app_url STRING,

  -- Pricing tier (for sophistication scoring)
  pricing_tier STRING,           -- 'free', 'starter', 'growth', 'enterprise'
  typical_monthly_cost FLOAT64,

  -- Metadata
  website_url STRING,
  description STRING,

  is_active BOOL DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================================
-- CHANGE TRACKING TABLES
-- ============================================================================

-- Unified change tracking table (replaces separate product_changes + store_changes)
-- All detected changes use a single schema with a scope discriminator.
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_changes` (
  change_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,

  -- Scope discriminator: 'product' (from change_detection_worker) or 'store' (from SQL queries)
  change_scope STRING NOT NULL DEFAULT 'product',

  detected_at TIMESTAMP,
  change_date DATE NOT NULL,
  change_type STRING NOT NULL,   -- 'tool_added', 'tool_removed', 'price_change', etc.
  change_category STRING,        -- 'tech_stack', 'pricing', 'products', 'content', 'funnel', 'marketing', 'policy'
  severity STRING,               -- 'critical', 'high', 'medium', 'low'

  -- Change details
  entity_type STRING,            -- 'pixel', 'app', 'product', 'price', 'variant'
  entity_id STRING,
  entity_name STRING,

  old_value STRING,
  new_value STRING,
  change_description STRING,     -- Human-readable description

  -- For numeric changes
  old_numeric_value FLOAT64,
  new_numeric_value FLOAT64,
  change_amount FLOAT64,
  change_percent FLOAT64,

  -- Significance (for store-scope changes from SQL detection)
  significance_score FLOAT64,

  -- Context
  previous_snapshot_id STRING,
  current_snapshot_id STRING,
  previous_snapshot_date DATE,
  current_snapshot_date DATE,

  -- Metadata
  metadata JSON,
  is_acknowledged BOOL DEFAULT FALSE,
  notification_sent BOOL DEFAULT FALSE,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY change_date
CLUSTER BY domain, change_scope, change_category, change_type;


-- ============================================================================
-- PROCESSING TABLES
-- ============================================================================

-- Enrichment queue
CREATE TABLE IF NOT EXISTS `shopify_intelligence.enrichment_queue` (
  queue_id STRING NOT NULL,
  store_id STRING NOT NULL,
  domain STRING NOT NULL,

  -- Queue management
  priority INT64 DEFAULT 0,      -- Higher = more urgent
  enrichment_tier STRING,        -- 'http', 'browser', 'both'
  snapshot_type STRING,          -- 'daily', 'weekly'
  activity_only BOOL DEFAULT FALSE,
  reason STRING,                 -- 'daily_monitor', 'weekly_refresh', 'http_blocked', 'manual'

  -- Status
  status STRING DEFAULT 'pending',  -- 'pending', 'processing', 'completed', 'failed'
  attempts INT64 DEFAULT 0,
  max_attempts INT64 DEFAULT 3,

  -- Processing
  locked_by STRING,              -- Worker ID
  locked_at TIMESTAMP,

  -- Timing
  scheduled_at TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,

  -- Error handling
  last_error STRING,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY status, priority;

-- Processing logs
CREATE TABLE IF NOT EXISTS `shopify_intelligence.processing_logs` (
  log_id STRING NOT NULL,
  store_id STRING,
  domain STRING,

  -- Job info
  job_type STRING,               -- 'http_enrichment', 'browser_enrichment', 'crawl_discovery'
  job_id STRING,
  worker_id STRING,

  -- Status
  status STRING,                 -- 'started', 'completed', 'failed'

  -- Performance
  duration_ms INT64,
  bytes_processed INT64,

  -- Details
  message STRING,
  error_details STRING,
  metadata JSON,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY job_type, status;

-- Store fetch state for conditional requests and backoff
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_fetch_state` (
  domain STRING NOT NULL,
  etag STRING,
  last_modified STRING,
  last_fetch_at TIMESTAMP,
  last_status INT64,
  tool_fingerprint STRING,
  consecutive_blocks INT64 DEFAULT 0,
  next_fetch_at TIMESTAMP,

  -- Proxy optimization: track domains that consistently succeed without proxy
  -- Skip proxy fallback if consecutive_direct_success > 5
  consecutive_direct_success INT64 DEFAULT 0,
  needs_proxy BOOL,  -- NULL = unknown, TRUE = needs proxy, FALSE = direct works

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY domain;


-- Store discovery staging (populated by WAT scanner, reconciled into stores)
CREATE TABLE IF NOT EXISTS `shopify_intelligence.store_discovery_staging` (
  domain STRING NOT NULL,
  crawl_id STRING NOT NULL,
  discovery_method STRING NOT NULL,  -- 'header', 'script_src', 'inline_js', 'meta_tag', 'myshopify_link'
  confidence FLOAT64 NOT NULL,
  discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY domain, crawl_id;


-- ============================================================================
-- INDEXES AND SEARCH OPTIMIZATION
-- ============================================================================

-- Create search index for full-text search on product titles/descriptions
-- Note: BigQuery Search is a separate feature that needs to be enabled
-- ALTER TABLE `shopify_intelligence.store_products_full`
-- ADD SEARCH INDEX product_search_idx
-- ON (title, description_text);


-- ============================================================================
-- USEFUL VIEWS
-- ============================================================================

-- Stores with their current tech stack
CREATE OR REPLACE VIEW `shopify_intelligence.store_tech_stack` AS
SELECT
  s.store_id,
  s.domain,
  s.sophistication_score,
  ARRAY_AGG(DISTINCT IF(d.detection_type = 'pixel', d.entity_name, NULL) IGNORE NULLS) as pixels,
  ARRAY_AGG(DISTINCT IF(d.detection_type = 'app', d.entity_name, NULL) IGNORE NULLS) as apps,
  COUNTIF(d.detection_type = 'pixel') as pixel_count,
  COUNTIF(d.detection_type = 'app') as app_count
FROM `shopify_intelligence.stores` s
LEFT JOIN `shopify_intelligence.detections` d
  ON s.store_id = d.store_id
  AND d.snapshot_date = (SELECT MAX(snapshot_date) FROM `shopify_intelligence.detections` WHERE store_id = s.store_id)
WHERE s.is_active = TRUE
GROUP BY s.store_id, s.domain, s.sophistication_score;

-- Stores by category of tools
CREATE OR REPLACE VIEW `shopify_intelligence.stores_by_tool_category` AS
SELECT
  s.domain,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'email') > 0 as has_email_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'sms') > 0 as has_sms_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'support') > 0 as has_support_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'subscriptions') > 0 as has_subscription_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'reviews') > 0 as has_reviews_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'loyalty') > 0 as has_loyalty_tool,
  COUNTIF(d.detection_type = 'app' AND d.entity_category = 'bnpl') > 0 as has_bnpl,
  COUNTIF(d.detection_type = 'pixel' AND d.entity_category = 'advertising') as advertising_pixels,
  COUNTIF(d.detection_type = 'pixel' AND d.entity_category = 'analytics') as analytics_tools,
  COUNTIF(d.detection_type = 'pixel' AND d.entity_category = 'heatmap') as cro_tools
FROM `shopify_intelligence.stores` s
LEFT JOIN `shopify_intelligence.detections` d
  ON s.store_id = d.store_id
  AND d.snapshot_date = (SELECT MAX(snapshot_date) FROM `shopify_intelligence.detections` WHERE store_id = s.store_id)
WHERE s.is_active = TRUE
GROUP BY s.domain;

-- Store change velocity for adaptive scheduling
-- Stores that change frequently get more frequent refreshes
CREATE OR REPLACE VIEW `shopify_intelligence.store_change_velocity` AS
SELECT
  s.store_id,
  s.domain,
  s.is_active,

  -- Count changes in last 30 days
  COALESCE((
    SELECT COUNT(*)
    FROM `shopify_intelligence.store_changes` pc
    WHERE pc.store_id = s.store_id
      AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ), 0) as change_count_30d,

  -- Count changes in last 7 days
  COALESCE((
    SELECT COUNT(*)
    FROM `shopify_intelligence.store_changes` pc
    WHERE pc.store_id = s.store_id
      AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ), 0) as change_count_7d,

  -- Is this a watched store?
  EXISTS(
    SELECT 1 FROM `shopify_intelligence.watched_stores` ws
    WHERE ws.store_id = s.store_id AND ws.is_active = TRUE
  ) as is_watched,

  -- Recommended refresh cadence based on change velocity
  CASE
    WHEN EXISTS(
      SELECT 1 FROM `shopify_intelligence.watched_stores` ws
      WHERE ws.store_id = s.store_id AND ws.is_active = TRUE
    ) THEN 'daily'  -- Watched stores always get daily checks
    WHEN COALESCE((
      SELECT COUNT(*)
      FROM `shopify_intelligence.store_changes` pc
      WHERE pc.store_id = s.store_id
        AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ), 0) > 2 THEN 'daily'  -- High-velocity: 3+ changes/month
    WHEN COALESCE((
      SELECT COUNT(*)
      FROM `shopify_intelligence.store_changes` pc
      WHERE pc.store_id = s.store_id
        AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ), 0) > 0 THEN 'weekly'  -- Some changes: 1-2/month
    ELSE 'monthly'  -- Stable: no changes in 30 days
  END as refresh_cadence,

  -- Days until next fetch based on cadence
  CASE
    WHEN EXISTS(
      SELECT 1 FROM `shopify_intelligence.watched_stores` ws
      WHERE ws.store_id = s.store_id AND ws.is_active = TRUE
    ) THEN 1
    WHEN COALESCE((
      SELECT COUNT(*)
      FROM `shopify_intelligence.store_changes` pc
      WHERE pc.store_id = s.store_id
        AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ), 0) > 2 THEN 1
    WHEN COALESCE((
      SELECT COUNT(*)
      FROM `shopify_intelligence.store_changes` pc
      WHERE pc.store_id = s.store_id
        AND pc.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ), 0) > 0 THEN 7
    ELSE 30
  END as refresh_interval_days,

  -- Last fetch info
  fs.last_fetch_at,
  fs.last_status,
  fs.consecutive_direct_success,
  fs.needs_proxy

FROM `shopify_intelligence.stores` s
LEFT JOIN `shopify_intelligence.store_fetch_state` fs
  ON s.domain = fs.domain
WHERE s.is_active = TRUE;


-- Product similarity candidates (stores with matching image hashes)
CREATE OR REPLACE VIEW `shopify_intelligence.similar_products` AS
SELECT
  a.domain as domain_a,
  a.product_handle as product_a,
  a.product_title as title_a,
  b.domain as domain_b,
  b.product_handle as product_b,
  b.product_title as title_b,
  a.image_url as image_a,
  b.image_url as image_b,
  a.phash
FROM `shopify_intelligence.product_images` a
JOIN `shopify_intelligence.product_images` b
  ON a.phash = b.phash
  AND a.domain < b.domain  -- Avoid duplicates
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM `shopify_intelligence.product_images`)
  AND b.snapshot_date = a.snapshot_date;


-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

/*
-- Find all wallet sellers
SELECT DISTINCT domain, COUNT(*) as wallet_products
FROM `shopify_intelligence.store_products_full`
WHERE 'wallet' IN UNNEST(tags)
   OR LOWER(product_type) LIKE '%wallet%'
   OR LOWER(title) LIKE '%wallet%'
GROUP BY domain
ORDER BY wallet_products DESC;

-- Stores using Klaviyo + Gorgias + Recharge (sophisticated stack)
SELECT s.domain
FROM `shopify_intelligence.stores` s
WHERE EXISTS (SELECT 1 FROM `shopify_intelligence.app_detections` ad WHERE ad.store_id = s.store_id AND ad.app_name = 'klaviyo')
  AND EXISTS (SELECT 1 FROM `shopify_intelligence.app_detections` ad WHERE ad.store_id = s.store_id AND ad.app_name = 'gorgias')
  AND EXISTS (SELECT 1 FROM `shopify_intelligence.app_detections` ad WHERE ad.store_id = s.store_id AND ad.app_name = 'recharge');

-- Stores shipping to UK
SELECT domain
FROM `shopify_intelligence.store_shipping`
WHERE 'UK' IN UNNEST(ships_to_countries)
   OR 'United Kingdom' IN UNNEST(ships_to_countries)
   OR ships_to_uk = TRUE;

-- Track sold out products
SELECT domain, handle, title
FROM `shopify_intelligence.store_products_full`
WHERE available_variants = 0
  AND total_variants > 0
  AND snapshot_date = CURRENT_DATE();

-- Detect tool adoption changes
SELECT
  domain,
  change_type,
  entity_name,
  change_date
FROM `shopify_intelligence.store_changes`
WHERE change_type IN ('tool_added', 'tool_removed')
  AND change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY change_date DESC;

-- Price change monitoring
SELECT
  domain,
  entity_name as product,
  old_numeric_value as old_price,
  new_numeric_value as new_price,
  change_percent,
  change_date
FROM `shopify_intelligence.store_changes`
WHERE change_type = 'price_change'
  AND ABS(change_percent) > 10  -- Significant changes only
ORDER BY change_date DESC;
*/
