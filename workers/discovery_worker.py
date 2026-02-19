#!/usr/bin/env python3
"""
Cloud Run Job: Shopify store discovery from Common Crawl WAT files.

Each task processes a partition of WAT file paths, scanning for Shopify
signatures in HTTP headers and HTML metadata. Discovered domains are
written to a BigQuery staging table for reconciliation.
"""

import gzip
import io
import json
import os
import re
import logging
import hashlib
import time
import traceback
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
PROJECT_ID = os.environ.get("PROJECT_ID", "shopifydb")
DATASET_ID = os.environ.get("DATASET_ID", "shopify_intelligence")
TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
CRAWL_ID = os.environ.get("CRAWL_ID", "CC-MAIN-2025-05")
SAMPLE_SIZE = int(os.environ.get("SAMPLE_SIZE", 0))  # 0 = all files
MAX_FILES_PER_TASK = int(os.environ.get("MAX_FILES_PER_TASK", 0))  # 0 = no limit
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 1))  # per-task concurrency
DETECTION_MODE = os.environ.get("DETECTION_MODE", "full")  # full | script_only
TASK_INDEX_LIST = os.environ.get("TASK_INDEX_LIST", "")  # comma-separated indices to retry
ORIGINAL_TASK_COUNT = int(os.environ.get("ORIGINAL_TASK_COUNT", TASK_COUNT))


def get_effective_task_index() -> tuple[int, int]:
    if not TASK_INDEX_LIST:
        return TASK_INDEX, TASK_COUNT
    indices = [int(i) for i in TASK_INDEX_LIST.split(",") if i.strip().isdigit()]
    if not indices:
        return TASK_INDEX, TASK_COUNT
    if TASK_INDEX >= len(indices):
        return -1, ORIGINAL_TASK_COUNT
    return indices[TASK_INDEX], ORIGINAL_TASK_COUNT

STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.store_discovery_staging"
STORES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.stores"
ENRICHMENT_QUEUE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.enrichment_queue"

CC_BASE_URL = "https://data.commoncrawl.org"

# Shopify detection patterns for WAT metadata
SHOPIFY_PATTERNS = {
    "header": re.compile(r"x-shopify-stage", re.IGNORECASE),
    "script_src": re.compile(
        r"(cdn\.shopify\.com|shopifycdn|shopifycloud|shopify\.com/s/files|cdn\.shopify\.com/s/files)",
        re.IGNORECASE,
    ),
    "inline_js": re.compile(r"Shopify\.shop", re.IGNORECASE),
    "meta_tag": re.compile(r'content=["\']Shopify["\']', re.IGNORECASE),
    "myshopify_link": re.compile(r"[a-z0-9-]+\.myshopify\.com", re.IGNORECASE),
}

RUN_RECONCILIATION = os.environ.get("RUN_RECONCILIATION", "0") == "1"
RUN_DEDUPE = os.environ.get("RUN_DEDUPE", "0") == "1"
RETRY_ATTEMPTS = int(os.environ.get("RETRY_ATTEMPTS", 3))
RETRY_BACKOFF = float(os.environ.get("RETRY_BACKOFF", 2.0))


def fetch_with_retries(url: str, timeout: int = 60) -> bytes:
    last_err = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            logger.warning(f"Fetch failed ({attempt}/{RETRY_ATTEMPTS}) for {url}: {e}")
            time.sleep(RETRY_BACKOFF ** attempt)
    raise RuntimeError(f"Failed to fetch {url}") from last_err


def get_latest_crawl_id() -> str:
    """Fetch latest Common Crawl index collection id from collinfo.json."""
    collinfo_url = "https://index.commoncrawl.org/collinfo.json"
    raw = fetch_with_retries(collinfo_url, timeout=30)
    collections = json.loads(raw.decode("utf-8"))

    def parse_id(crawl_id: str) -> tuple[int, int]:
        # CC-MAIN-YYYY-NN
        try:
            parts = crawl_id.split("-")
            return (int(parts[2]), int(parts[3]))
        except Exception:
            return (0, 0)

    candidates = [c for c in collections if c.get("id", "").startswith("CC-MAIN-")]
    if not candidates:
        return "CC-MAIN-2025-05"
    latest = max(candidates, key=lambda c: parse_id(c.get("id", "")))
    return latest.get("id", "CC-MAIN-2025-05")


def get_wat_paths(crawl_id: str) -> list[str]:
    """Fetch the list of WAT file paths for a given Common Crawl release."""
    paths_url = f"{CC_BASE_URL}/crawl-data/{crawl_id}/wat.paths.gz"
    logger.info(f"Fetching WAT paths from {paths_url}")
    raw = fetch_with_retries(paths_url, timeout=120)
    decompressed = gzip.decompress(raw).decode("utf-8")
    paths = [p.strip() for p in decompressed.strip().split("\n") if p.strip()]
    logger.info(f"Found {len(paths)} WAT files for {crawl_id}")
    return paths


def partition_paths(paths: list[str]) -> list[str]:
    """Return the subset of paths assigned to this task."""
    if SAMPLE_SIZE > 0:
        paths = paths[:SAMPLE_SIZE]
    effective_index, effective_count = get_effective_task_index()
    if effective_index < 0:
        return []
    my_paths = [p for i, p in enumerate(paths) if i % effective_count == effective_index]
    if MAX_FILES_PER_TASK > 0:
        my_paths = my_paths[:MAX_FILES_PER_TASK]
    logger.info(f"Task {TASK_INDEX}/{TASK_COUNT}: assigned {len(my_paths)} WAT files")
    return my_paths


def extract_domain(url: str) -> Optional[str]:
    """Extract and normalize domain from a URL."""
    try:
        if not url:
            return None
        # urlparse needs a scheme to reliably parse netloc
        parsed = urlparse(url if "://" in url else f"//{url}", scheme="http")
        domain = parsed.netloc or parsed.path.split("/", 1)[0]
        domain = domain.split(":", 1)[0].lower().strip().rstrip(".")
        if domain.startswith("www."):
            domain = domain[4:]
        if "." in domain and len(domain) > 3 and not domain.endswith(".myshopify.com"):
            return domain
    except Exception:
        return None
    return None


def header_has_shopify(headers: object) -> bool:
    """Detect Shopify headers in a case-insensitive way."""
    if isinstance(headers, dict):
        for key in headers.keys():
            if str(key).lower() == "x-shopify-stage":
                return True
        return False
    if isinstance(headers, list):
        for item in headers:
            if isinstance(item, (list, tuple)) and item:
                if str(item[0]).lower() == "x-shopify-stage":
                    return True
        return False
    return False


def detect_shopify_in_record(record_json: dict) -> Optional[tuple[str, str]]:
    """
    Check a single WAT record for Shopify signatures.
    Returns (domain, detection_method) or None.
    """
    try:
        envelope = record_json.get("Envelope", {})
        payload = envelope.get("Payload-Metadata", {})
        http_meta = payload.get("HTTP-Response-Metadata", {})

        # Get target URL
        target_uri = envelope.get("WARC-Header-Metadata", {}).get("WARC-Target-URI", "")
        domain = extract_domain(target_uri)
        if not domain:
            return None

        # Optional fast mode: script-only detection
        if DETECTION_MODE == "script_only":
            html_meta = http_meta.get("HTML-Metadata", {})
            head = html_meta.get("Head", {})
            scripts = head.get("Scripts", []) or html_meta.get("Scripts", [])
            for script in scripts:
                src = script.get("url", "") or script.get("src", "")
                if SHOPIFY_PATTERNS["script_src"].search(src):
                    return (domain, "script_src")
            return None

        # Check HTTP headers for x-shopify-stage
        headers = http_meta.get("Headers", {})
        if header_has_shopify(headers):
            return (domain, "header")
        headers_str = json.dumps(headers) if isinstance(headers, dict) else str(headers)
        if SHOPIFY_PATTERNS["header"].search(headers_str):
            return (domain, "header")

        # Check HTML metadata (nested inside HTTP-Response-Metadata)
        html_meta = http_meta.get("HTML-Metadata", {})
        head = html_meta.get("Head", {})

        # Check scripts (Head.Scripts and HTML-Metadata.Scripts)
        scripts = head.get("Scripts", []) or html_meta.get("Scripts", [])
        for script in scripts:
            src = script.get("url", "") or script.get("src", "")
            if SHOPIFY_PATTERNS["script_src"].search(src):
                return (domain, "script_src")

        # Check meta tags (Head.Metas and HTML-Metadata.Metas)
        metas = head.get("Metas", []) or html_meta.get("Metas", [])
        for meta in metas:
            content = meta.get("content", "")
            if SHOPIFY_PATTERNS["meta_tag"].search(content):
                return (domain, "meta_tag")

        # Check links for myshopify.com references (Head.Links and HTML-Metadata.Links)
        links = head.get("Links", []) or html_meta.get("Links", [])
        for link in links:
            href = link.get("url", "") or link.get("href", "")
            if SHOPIFY_PATTERNS["myshopify_link"].search(href):
                return (domain, "myshopify_link")

        # Check inline content (title, scripts text)
        head_str = json.dumps(head) if head else ""
        if SHOPIFY_PATTERNS["inline_js"].search(head_str):
            return (domain, "inline_js")

    except Exception as e:
        logger.debug(f"Error parsing record: {e}")
    return None


def process_wat_file(wat_path: str) -> set[tuple[str, str]]:
    """
    Download and scan a single WAT file for Shopify domains.
    Uses warcio to properly parse multi-member gzip WARC records.
    Returns set of (domain, method) tuples.
    """
    from warcio.archiveiterator import ArchiveIterator

    url = f"{CC_BASE_URL}/{wat_path}"
    discovered = set()

    try:
        resp = requests.get(url, timeout=300, stream=True)
        resp.raise_for_status()

        records_parsed = 0
        metadata_records = 0
        for record in ArchiveIterator(resp.raw):
            records_parsed += 1
            if record.rec_type != "metadata":
                continue
            metadata_records += 1
            try:
                content = record.content_stream().read().decode("utf-8", errors="replace")
                if not content or not content.strip().startswith("{"):
                    continue
                try:
                    record_json = json.loads(content.strip())
                    result = detect_shopify_in_record(record_json)
                    if result:
                        discovered.add(result)
                except json.JSONDecodeError:
                    continue
            except Exception as e:
                logger.debug(f"Error reading record: {e}")
        if records_parsed < 10:
            logger.warning(f"Only {records_parsed} records parsed from {wat_path} ({metadata_records} metadata)")

    except requests.exceptions.RequestException as e:
        logger.error(f"WAT download failed: {wat_path} ({url}) -> {e}")
        logger.debug(traceback.format_exc())
    except Exception as e:
        logger.error(f"WAT processing failed: {wat_path} ({url}) -> {e}")
        logger.debug(traceback.format_exc())

    return discovered


def write_to_bigquery(client: bigquery.Client, discoveries: list[dict]):
    """Write discovered domains to staging table via streaming inserts."""
    if not discoveries:
        return

    # Batch in groups of 500
    batch_size = 500
    for i in range(0, len(discoveries), batch_size):
        batch = discoveries[i:i + batch_size]
        errors = client.insert_rows_json(STAGING_TABLE, batch)
        if errors:
            logger.error(f"BigQuery insert errors: {errors[:3]}")
        else:
            logger.info(f"Inserted {len(batch)} rows to staging table")


def run_reconciliation(client: bigquery.Client):
    """
    Reconcile staging table with stores table.
    Insert only new domains not already in stores.
    """
    logger.info("Running reconciliation query...")

    reconcile_sql = f"""
    DECLARE new_count INT64;
    DECLARE queue_count INT64;

    CREATE TEMP TABLE new_domains AS
    SELECT
      REGEXP_REPLACE(LOWER(s.domain), r'^www\\.', '') as domain,
      s.crawl_id as crawl_id
    FROM `{STAGING_TABLE}` s
    LEFT JOIN `{STORES_TABLE}` e ON REGEXP_REPLACE(LOWER(s.domain), r'^www\\.', '') = REGEXP_REPLACE(LOWER(e.domain), r'^www\\.', '')
    WHERE e.domain IS NULL
      AND s.domain NOT LIKE '%.myshopify.com'
      AND s.confidence >= 0.9
    GROUP BY domain, crawl_id;

    SET new_count = (SELECT COUNT(*) FROM new_domains);

    INSERT INTO `{STORES_TABLE}` (store_id, domain, source_crawl, discovery_method, is_active, first_seen_at, created_at, updated_at)
    SELECT
      TO_HEX(SHA256(domain)),
      domain,
      crawl_id,
      'common_crawl_wat',
      TRUE,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    FROM new_domains;

    CREATE TEMP TABLE queue_candidates AS
    SELECT
      SUBSTR(TO_HEX(SHA256(CONCAT(domain, ':new_store:', CAST(CURRENT_TIMESTAMP() AS STRING)))), 1, 32) as queue_id,
      TO_HEX(SHA256(domain)) as store_id,
      domain
    FROM new_domains nd
    WHERE NOT EXISTS (
      SELECT 1
      FROM `{ENRICHMENT_QUEUE_TABLE}` q
      WHERE q.store_id = TO_HEX(SHA256(nd.domain))
        AND q.snapshot_type = 'new_store'
        AND q.status IN ('pending', 'processing')
    );

    SET queue_count = (SELECT COUNT(*) FROM queue_candidates);

    INSERT INTO `{ENRICHMENT_QUEUE_TABLE}`
      (queue_id, store_id, domain, priority, enrichment_tier, snapshot_type, activity_only, scheduled_at, reason)
    SELECT
      queue_id,
      store_id,
      domain,
      200 as priority,
      'http' as enrichment_tier,
      'new_store' as snapshot_type,
      FALSE as activity_only,
      CURRENT_TIMESTAMP() as scheduled_at,
      'new_store_discovery' as reason
    FROM queue_candidates;

    SELECT new_count as new_stores_added, queue_count as queued_for_enrichment;
    """

    job = client.query(reconcile_sql)
    result = list(job.result())
    if result:
        logger.info(
            "Reconciliation complete. New stores: %s. Queued for enrichment: %s",
            result[0].new_stores_added,
            result[0].queued_for_enrichment,
        )
    else:
        logger.info("Reconciliation complete. No new stores found.")


def run_dedupe(client: bigquery.Client):
    """
    De-dupe stores table by canonicalized domain (case-insensitive).
    Keeps the most recently updated row for each domain.
    """
    logger.info("Running stores de-duplication...")
    dedupe_sql = f"""
    DELETE FROM `{STORES_TABLE}`
    WHERE store_id IN (
      SELECT store_id
      FROM (
        SELECT
          store_id,
          ROW_NUMBER() OVER (
            PARTITION BY REGEXP_REPLACE(LOWER(domain), r'^www\\.', '')
            ORDER BY updated_at DESC, created_at DESC
          ) AS rn
        FROM `{STORES_TABLE}`
      )
      WHERE rn > 1
    );
    """
    job = client.query(dedupe_sql)
    job.result()

    # Log post-dedupe stats
    stats_sql = f"""
    SELECT
      (SELECT COUNT(DISTINCT domain) FROM `{STAGING_TABLE}` WHERE crawl_id = '{CRAWL_ID}') as total_discovered,
      (SELECT COUNT(*) FROM `{STORES_TABLE}`) as total_stores
    """
    stats = list(client.query(stats_sql).result())
    if stats:
        row = stats[0]
        logger.info(
            "De-duplication complete. Discovered: %s, Total stores: %s",
            row.total_discovered,
            row.total_stores,
        )
    else:
        logger.info("De-duplication complete.")


def ensure_staging_table(client: bigquery.Client):
    """Create staging table if it doesn't exist."""
    schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("discovery_method", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("confidence", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("discovered_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    table_ref = bigquery.Table(STAGING_TABLE, schema=schema)
    try:
        client.create_table(table_ref)
        logger.info(f"Created staging table {STAGING_TABLE}")
    except Exception:
        logger.info(f"Staging table {STAGING_TABLE} already exists")


def main():
    global CRAWL_ID
    if CRAWL_ID.lower() == "latest":
        CRAWL_ID = get_latest_crawl_id()

    effective_index, effective_count = get_effective_task_index()
    logger.info(f"=== Shopify Discovery Worker ===")
    logger.info(f"Task {TASK_INDEX}/{TASK_COUNT} (effective {effective_index}/{effective_count}), Crawl: {CRAWL_ID}, Sample: {SAMPLE_SIZE or 'full'}")
    logger.info(f"Reconciliation: {'enabled' if RUN_RECONCILIATION else 'disabled'}")
    logger.info(f"De-duplication: {'enabled' if RUN_DEDUPE else 'disabled'}")

    client = bigquery.Client(project=PROJECT_ID)

    # Ensure staging table exists (all tasks try, only first succeeds)
    ensure_staging_table(client)

    # Get WAT file paths and partition
    all_paths = get_wat_paths(CRAWL_ID)
    my_paths = partition_paths(all_paths)

    if not my_paths:
        logger.info("No WAT files assigned to this task")
        return

    # Process WAT files, flushing to BigQuery every 10 files
    all_discovered: dict[str, str] = {}  # domain -> method (keeps first detection)
    pending_flush: dict[str, str] = {}   # domains not yet written to BQ
    files_processed = 0
    files_with_hits = 0
    total_written = 0

    def handle_discoveries(path: str, discoveries: set[tuple[str, str]], elapsed: float):
        nonlocal files_processed, files_with_hits, total_written
        if discoveries:
            files_with_hits += 1
            for domain, method in discoveries:
                if domain not in all_discovered:
                    all_discovered[domain] = method
                    pending_flush[domain] = method

        files_processed += 1
        if files_processed % 10 == 0:
            logger.info(
                f"Progress: {files_processed}/{len(my_paths)} files, "
                f"{len(all_discovered)} domains found, last file {elapsed:.1f}s"
            )
            if pending_flush:
                now = datetime.utcnow().isoformat() + "Z"
                rows = [
                    {
                        "domain": domain,
                        "crawl_id": CRAWL_ID,
                        "discovery_method": method,
                        "confidence": 1.0,
                        "discovered_at": now,
                    }
                    for domain, method in pending_flush.items()
                ]
                write_to_bigquery(client, rows)
                total_written += len(pending_flush)
                logger.info(f"Flushed {len(pending_flush)} domains (total written: {total_written})")
                pending_flush.clear()

    if MAX_WORKERS <= 1:
        for path in my_paths:
            start = time.time()
            discoveries = process_wat_file(path)
            elapsed = time.time() - start
            handle_discoveries(path, discoveries, elapsed)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info(f"Using per-task concurrency: MAX_WORKERS={MAX_WORKERS}")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_path = {}
            start_times = {}
            for path in my_paths:
                future = executor.submit(process_wat_file, path)
                future_to_path[future] = path
                start_times[future] = time.time()
            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    discoveries = future.result()
                except Exception as e:
                    logger.error(f"Task failed processing {path}: {e}")
                    logger.debug(traceback.format_exc())
                    discoveries = set()
                elapsed = time.time() - start_times.get(future, time.time())
                handle_discoveries(path, discoveries, elapsed)

    # Final flush
    if pending_flush:
        now = datetime.utcnow().isoformat() + "Z"
        rows = [
            {
                "domain": domain,
                "crawl_id": CRAWL_ID,
                "discovery_method": method,
                "confidence": 1.0,
                "discovered_at": now,
            }
            for domain, method in pending_flush.items()
        ]
        write_to_bigquery(client, rows)
        total_written += len(pending_flush)

    logger.info(
        f"Scan complete: {files_processed} files, {files_with_hits} with hits, "
        f"{len(all_discovered)} unique domains, {total_written} written to BQ"
    )

    # Optional reconciliation (should run after all tasks complete)
    if TASK_INDEX == 0 and RUN_RECONCILIATION:
        logger.info("Task 0: running reconciliation (ensure all tasks are complete)")
        run_reconciliation(client)
        if RUN_DEDUPE:
            logger.info("Task 0: running de-duplication")
            run_dedupe(client)

    logger.info(f"Task {TASK_INDEX} complete")


if __name__ == "__main__":
    main()
