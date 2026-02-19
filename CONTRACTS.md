# Discovery Service Contract

## Purpose
Find new Shopify stores via Common Crawl WAT scanning. Self-contained — no dependencies on other services.

> **Schema Details:** See [SCHEMA.md](./SCHEMA.md) for full column-level BigQuery table contracts, breaking change policies, and downstream consumer impact matrix.

## Workers
| Worker | Trigger | Description |
|--------|---------|-------------|
| `discovery_worker.py` | Cloud Run Job (manual/scheduled) | Scans Common Crawl WAT files for Shopify domains |

## BigQuery Tables

### Writes To
| Table | Operation | Notes |
|-------|-----------|-------|
| `stores` | INSERT/UPDATE | Master store registry (reconciliation) |
| `store_discovery_staging` | INSERT | Raw discovered domains before reconciliation |

### Reads From
| Table | Purpose |
|-------|---------|
| `stores` | Deduplication during reconciliation |

## Pub/Sub
| Topic | Direction | Purpose |
|-------|-----------|---------|
| `new-stores-discovered` | Publishes | After reconciliation completes |

## Environment Variables
| Var | Required | Default |
|-----|----------|---------|
| `PROJECT_ID` | Yes | `shopifydb` |
| `DATASET_ID` | Yes | `shopify_intelligence` |
| `CRAWL_ID` | Yes | e.g. `CC-MAIN-2025-05` |
| `CLOUD_RUN_TASK_INDEX` | Auto | Set by Cloud Run |
| `CLOUD_RUN_TASK_COUNT` | Auto | Set by Cloud Run |
