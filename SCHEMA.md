# Discovery Service - BigQuery Schema Contract

> **Owner:** Discovery Service
> **Dataset:** `shopify_intelligence`
> **Last Updated:** 2026-02-19

## Overview

Discovery is the **entry point** for all store data. It owns the master store registry and the staging table for raw crawl results. All other services treat Discovery's tables as read-only upstream dependencies.

---

## Tables I Own (Write)

### `stores` — Master Store Registry

**Operations:** INSERT (new stores), UPDATE (reconciliation, status changes)
**Partitioned By:** `DATE(created_at)` | **Clustered By:** `domain, is_active`

| Column | Type | Description |
|--------|------|-------------|
| `store_id` | STRING | PK — SHA256 hash of normalized domain |
| `domain` | STRING | Normalized domain (no www, lowercase) |
| `original_domain` | STRING | Original domain as discovered |
| `first_seen_at` | TIMESTAMP | When store was first discovered |
| `last_seen_at` | TIMESTAMP | Last time seen in a crawl |
| `source_crawl` | STRING | e.g., `CC-MAIN-2025-05` |
| `discovery_method` | STRING | `common_crawl`, `manual`, `referral` |
| `is_active` | BOOL | Whether store is currently reachable |
| `last_http_status` | INT64 | Last HTTP status from enrichment |
| `last_checked_at` | TIMESTAMP | Last enrichment check time |
| `consecutive_failures` | INT64 | Count of consecutive failed fetches |
| `estimated_size` | STRING | `small`, `medium`, `large`, `enterprise` |
| `sophistication_score` | FLOAT64 | Calculated maturity score 0-100 |
| `last_deep_enriched_at` | TIMESTAMP | Last deep enrichment timestamp |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Row last updated |

#### Breaking Change Policy
- **Safe changes:** Adding new columns (additive)
- **Requires coordination with Enrichment + API:**
  - Renaming `domain`, `store_id`, `is_active` (used in all enrichment worker queries)
  - Removing `last_deep_enriched_at` (used in deep_enrichment scheduling)
  - Changing `store_id` generation logic (referenced as FK across all enrichment tables)

---

### `store_discovery_staging` — Raw Crawl Results

**Operations:** INSERT (streaming during crawl)
**Clustered By:** `domain, crawl_id`
**Consumers:** Discovery only (internal reconciliation)

| Column | Type | Description |
|--------|------|-------------|
| `domain` | STRING | Discovered domain |
| `crawl_id` | STRING | Common Crawl identifier |
| `discovery_method` | STRING | `header`, `script_src`, `inline_js`, `meta_tag`, `myshopify_link` |
| `confidence` | FLOAT64 | Detection confidence 0-1 |
| `discovered_at` | TIMESTAMP | When discovered |

> **No external consumers.** This table is internal to Discovery's reconciliation pipeline. Changes here have zero downstream impact.

---

### `enrichment_queue` — Initial Population

**Operations:** INSERT (enqueue newly discovered stores after reconciliation)
**Note:** Enrichment also writes to this table (status updates). See Enrichment SCHEMA.md.

| Column | Type | Description |
|--------|------|-------------|
| `queue_id` | STRING | PK |
| `store_id` | STRING | FK → stores.store_id |
| `domain` | STRING | Store domain |
| `priority` | INT64 | Higher = more urgent |
| `enrichment_tier` | STRING | `http`, `browser`, `both` |
| `snapshot_type` | STRING | `daily`, `weekly` |
| `activity_only` | BOOL | Activity-only check |
| `reason` | STRING | `daily_monitor`, `weekly_refresh`, `http_blocked`, `manual` |
| `status` | STRING | `pending`, `processing`, `completed`, `failed` |
| `attempts` | INT64 | Attempt count |
| `max_attempts` | INT64 | Max retries |
| `locked_by` | STRING | Worker ID |
| `locked_at` | TIMESTAMP | Lock time |
| `scheduled_at` | TIMESTAMP | Scheduled execution time |
| `started_at` | TIMESTAMP | Processing start |
| `completed_at` | TIMESTAMP | Processing end |
| `last_error` | STRING | Last error message |
| `created_at` | TIMESTAMP | Row creation |
| `updated_at` | TIMESTAMP | Last update |

#### Shared Ownership Note
This table has **dual writers**: Discovery INSERTs new entries, Enrichment UPDATEs status/locking fields. Neither service should add/remove columns without coordinating with the other.

---

## Tables I Read (Consume)

### `stores` (self-read)

**Purpose:** Deduplication during reconciliation — avoid re-inserting domains already in the registry.

**Columns I depend on:**
| Column | Usage |
|--------|-------|
| `domain` | Exact match for deduplication |
| `store_id` | Reference for enrichment_queue FK |

---

## Pub/Sub Topics

| Topic | Direction | Payload | Consumers |
|-------|-----------|---------|-----------|
| `new-stores-discovered` | **Publishes** | `{ crawl_id, store_count }` | Enrichment (trigger scheduling) |

---

## Views I Own

None. Discovery has no views.

---

## Impact Matrix

If you change a Discovery-owned table, check these consumers:

| Table | Consumer Service | Columns Used | Impact of Change |
|-------|-----------------|--------------|------------------|
| `stores` | Enrichment (8 workers) | `store_id`, `domain`, `is_active`, `last_deep_enriched_at` | All enrichment scheduling breaks |
| `stores` | API (monitor routes) | `is_active`, `last_deep_enriched_at` | Freshness monitoring breaks |
| `enrichment_queue` | Enrichment (scheduler) | `store_id`, `status`, `snapshot_type`, `activity_only`, `scheduled_at` | Queue scheduling breaks |
