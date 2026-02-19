# BigQuery Schema Ownership Index

> **Dataset:** `shopify_intelligence`
> **Last Updated:** 2026-02-19

This file is the **single source of truth** for which service owns which BigQuery table. Before modifying any table schema, check this file to understand who writes to it and who reads from it.

## Ownership Rules

1. **Every table has exactly ONE owning service** (the only service that writes to it)
2. **Consumers treat other services' tables as read-only** — never write to a table you don't own
3. **Additive changes are safe** — adding new columns won't break consumers
4. **Breaking changes require coordination** — renaming/removing columns used by consumers requires updating consumer SCHEMA.md files first
5. **The `enrichment_queue` table is the ONE exception** — it has shared ownership (Discovery INSERTs, Enrichment UPDATEs)

---

## Quick Reference

### Discovery Service Owns
| Table | Consumers | Schema File |
|-------|-----------|-------------|
| `stores` | Enrichment, API | `sql/bigquery_schema_v2.sql` |
| `store_discovery_staging` | None (internal) | `sql/bigquery_schema_v2.sql` |
| `enrichment_queue` (INSERT) | Enrichment (UPDATE) | `sql/bigquery_schema_v2.sql` |

### Enrichment Service Owns
| Table | Consumers | Schema File |
|-------|-----------|-------------|
| `store_snapshots` | API | `sql/bigquery_schema_v2.sql` |
| `detections` | API | `sql/bigquery_schema_v2.sql` |
| `store_products` | API (via cache) | `sql/bigquery_schema_v2.sql` |
| `store_products_full` | Enrichment (embeddings) | `sql/bigquery_schema_v2.sql` |
| `product_images` | Enrichment (similarity) | `sql/bigquery_schema_v2.sql` |
| `store_business_info` | API (via cache) | `sql/bigquery_schema_v2.sql` |
| `store_categories` | API (via cache) | `sql/store_categories_schema.sql` |
| `store_authority` | API (via cache) | `sql/store_authority_schema.sql` |
| `store_ranking_signal_snapshots` | API (via cache) | `sql/store_authority_schema.sql` |
| `store_changes` | API | `sql/bigquery_schema_v2.sql` |
| `watched_stores` | Enrichment | `sql/change_detection_schema.sql` |
| `change_detection_runs` | Enrichment (monitoring) | `sql/change_detection_schema.sql` |
| `store_screenshots` | API | `sql/change_detection_schema.sql` |
| `store_landing_pages` | Enrichment | `sql/landing_pages_schema.sql` |
| `product_text_embeddings` | Enrichment | `sql/product_embeddings_schema.sql` |
| `store_embeddings` | Enrichment | `sql/store_similarity_schema.sql` |
| `store_similarity` | API | `sql/store_similarity_schema.sql` |
| `store_projections` | Enrichment | `sql/store_similarity_schema.sql` |
| `store_search_cache` | **API (primary)** | `sql/serving_layer_schema.sql` |
| `enrichment_queue` (UPDATE) | Enrichment | `sql/bigquery_schema_v2.sql` |
| `store_fetch_state` | Enrichment | `sql/bigquery_schema_v2.sql` |
| `enrichment_fetch_metrics` | Enrichment | `sql/bigquery_schema_v2.sql` |
| `processing_logs` | Enrichment | `sql/bigquery_schema_v2.sql` |
| `enrichment_job_runs` | Enrichment | `sql/enrichment_monitoring_schema.sql` |

### API Service Owns
| Table | Consumers | Schema File |
|-------|-----------|-------------|
| `store_alerts` | None (API only) | (defined in `bigquery.ts`) |

### System Setup (No Service Owner)
| Table | Consumers | Schema File |
|-------|-----------|-------------|
| `pixel_definitions` | Enrichment | `sql/bigquery_schema_v2.sql` |
| `app_definitions` | Enrichment, API | `sql/bigquery_schema_v2.sql` |

---

## Data Flow Diagram

```
                    ┌─────────────────────┐
                    │     DISCOVERY        │
                    │                     │
                    │  Writes:            │
                    │  • stores           │
                    │  • staging          │
                    │  • enrichment_queue │
                    └─────────┬───────────┘
                              │
                    Pub/Sub: new-stores-discovered
                              │
                              ▼
                    ┌─────────────────────┐
                    │     ENRICHMENT       │
                    │                     │
                    │  Reads:             │
                    │  • stores           │ ◄── From Discovery
                    │  • enrichment_queue │ ◄── From Discovery
                    │                     │
                    │  Writes:            │
                    │  • store_snapshots  │
                    │  • detections       │
                    │  • store_products*  │
                    │  • store_changes    │
                    │  • store_authority  │
                    │  • store_categories │
                    │  • store_search_    │
                    │    cache            │
                    │  • (~20 tables)     │
                    └─────────┬───────────┘
                              │
                    Pub/Sub: enrichment-complete
                              │
                              ▼
                    ┌─────────────────────┐
                    │        API          │
                    │                     │
                    │  Reads:             │
                    │  • store_search_    │ ◄── From Enrichment
                    │    cache (primary)  │
                    │  • detections       │ ◄── From Enrichment
                    │  • store_changes    │ ◄── From Enrichment
                    │  • stores           │ ◄── From Discovery
                    │  • (~12 tables)     │
                    │                     │
                    │  Writes:            │
                    │  • store_alerts     │ (self-contained)
                    └─────────────────────┘
```

---

## Breaking Change Checklist

Before renaming or removing a column:

1. Check the owning service's `SCHEMA.md` for the **Impact Matrix** section
2. Find ALL consumer services that read that column
3. Update consumer code FIRST (or coordinate a synchronized release)
4. Update the SQL schema file
5. Run the migration
6. Update this OWNERSHIP.md file

### Highest-Risk Columns (Used Across All Services)

| Column | Table | Used By |
|--------|-------|---------|
| `domain` | `stores` | Discovery, Enrichment (8 workers), API |
| `store_id` | `stores` | Discovery, Enrichment (all), API |
| `is_active` | `stores` | Enrichment (all workers), API |
| `domain` | `store_search_cache` | API (every query) |
| `tools` | `store_search_cache` | API (search, filter, display) |
| `pixels` | `store_search_cache` | API (search, filter, display) |
