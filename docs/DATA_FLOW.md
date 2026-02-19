# Cross-Service Data Flow & Schema Boundaries

> **Last Updated:** 2026-02-19

## Service Boundary Summary

```
┌─────────────────────────────────────────────────────────────────────┐
│                     BigQuery Dataset: shopify_intelligence          │
│                                                                     │
│  ┌──────────────┐   ┌──────────────────────┐   ┌──────────────┐   │
│  │  DISCOVERY    │   │     ENRICHMENT        │   │     API      │   │
│  │              │   │                      │   │              │   │
│  │  WRITES:     │   │  WRITES:             │   │  WRITES:     │   │
│  │  3 tables    │──▶│  25+ tables          │──▶│  1 table     │   │
│  │              │   │                      │   │              │   │
│  │  READS:      │   │  READS:              │   │  READS:      │   │
│  │  1 table     │   │  5 external tables   │   │  14 tables   │   │
│  │  (self)      │   │  (from Discovery)    │   │  (from Enr.) │   │
│  └──────────────┘   └──────────────────────┘   └──────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Cross-Service Contracts

### Discovery → Enrichment

| Interface | Type | Details |
|-----------|------|---------|
| `stores` table | BQ Table | Enrichment reads `store_id`, `domain`, `is_active`, `last_deep_enriched_at` |
| `enrichment_queue` table | BQ Table | Discovery INSERTs new entries; Enrichment UPDATEs status |
| `new-stores-discovered` | Pub/Sub | Payload: `{ crawl_id, domains[], count, timestamp }` |

**Breaking change risk:** LOW — Discovery writes to 3 tables, and only 4 columns from `stores` are consumed by enrichment.

### Enrichment → API

| Interface | Type | Details |
|-----------|------|---------|
| `store_search_cache` | BQ Table | **PRIMARY** — API reads ~50 columns. P0 if missing. |
| `app_detections` | BQ Table | 5 columns: `app_id`, `app_name`, `app_category`, `domain`, `snapshot_date` |
| `pixel_detections` | BQ Table | 5 columns: `pixel_id`, `pixel_name`, `pixel_category`, `domain`, `snapshot_date` |
| `product_changes` | BQ Table | 11 columns for change feed |
| `store_changes` | BQ Table | 9 columns for store history |
| `store_snapshots` | BQ Table | 16 columns for detail views |
| `store_similarity` | BQ Table | 8 columns for similar stores |
| `store_screenshots` | BQ Table | 4 columns for screenshot display |
| `enrichment_queue` | BQ Table | 5 columns for monitor dashboard |
| `tool_usage_daily` | BQ View | 5 columns for trend charts |
| `tool_wins_losses_daily` | BQ View | 6 columns for tool analytics |
| `tool_switch_events` | BQ View | 6 columns for migration analysis |
| `enrichment-complete` | Pub/Sub | Payload: `{ batch_id, tier, domains_processed }` |

**Breaking change risk:** HIGH — Enrichment writes to 25+ tables that API reads. The `store_search_cache` is the most critical — if `cache_refresh.py` changes column names, the entire API breaks.

### Discovery → API

| Interface | Type | Details |
|-----------|------|---------|
| `stores` | BQ Table | API reads `is_active`, `last_deep_enriched_at` (monitor only) |

**Breaking change risk:** LOW — API only reads 2 columns from `stores` for the monitor dashboard.

---

## Schema Change Protocol

### Safe Changes (No Coordination Needed)
- Adding a new column to a table you own
- Adding a new table
- Adding a new view
- Changing default values on columns consumers don't read

### Requires Consumer Notification
- Adding a NOT NULL column (may break INSERTs from other services)
- Changing column types (even if compatible, consumers should validate)
- Changing partitioning or clustering (may affect query performance)

### Requires Coordinated Release
- Renaming a column that consumers read
- Removing a column that consumers read
- Changing `store_search_cache` schema (requires API + `cache_refresh.py` update)
- Changing `stores` table schema (requires all 3 services)
- Changing `enrichment_queue` schema (requires Discovery + Enrichment)

---

## Per-Service Schema Documentation

| Service | Contract | Schema | SQL |
|---------|----------|--------|-----|
| Discovery | [CONTRACTS.md](../services/discovery/CONTRACTS.md) | [SCHEMA.md](../services/discovery/SCHEMA.md) | [owned_tables.sql](../services/discovery/sql/owned_tables.sql) |
| Enrichment | [CONTRACTS.md](../services/enrichment/CONTRACTS.md) | [SCHEMA.md](../services/enrichment/SCHEMA.md) | [owned_tables.sql](../services/enrichment/sql/owned_tables.sql) |
| API | [CONTRACTS.md](../services/api/CONTRACTS.md) | [SCHEMA.md](../services/api/SCHEMA.md) | N/A (reads only) |

## Full Schema Reference

All SQL CREATE TABLE statements: [shared/schemas/](./schemas/) and [sql/](../sql/)
