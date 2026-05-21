# Fail-Fast Table Processing: S3 403 Timeout Issue

## Problem

When the catalog-sync job encounters an S3 `403 Forbidden` (invalid/expired credentials) during table processing, a single table takes ~93 seconds to fail. This happens because:

1. `scrapeTable()` calls `describeTable()` which runs `DESCRIBE EXTENDED` via Spark SQL.
2. Spark/Iceberg resolves the table through `RESTCatalog.loadTable` -> S3.
3. The AWS SDK retries the 403 error multiple times with exponential backoff (default: 3 retries).
4. Iceberg makes multiple S3 calls during `loadTable` (metadata files, manifest lists), each retrying independently.
5. The cumulative effect of all retries across all S3 operations produces the ~93s delay.

### Error Chain (from logs)

```
LakehouseMetadataExtractor - Processing table: dit_dads_budp_bkp.wd_dads_run.ss_ops_review_ecosystem_availability_sdp
  -> Analyzer$ResolveRelations -> CatalogV2Util.loadTable
    -> SparkCatalog.loadTable -> CachingCatalog -> RESTCatalog.loadTable
      -> S3Exception: The Access Key Id you provided does not exist in our records.
         (Service: S3, Status Code: 403)
```

### Impact

- A 403 is a **non-retryable client error** — retrying with the same bad credentials will never succeed.
- Since this is a generic job, any catalog/table can hit this if credentials are misconfigured.
- With many affected tables, the total job runtime can balloon significantly.

## Current Behavior

In `LakehouseMetadataExtractor.kt`, the `describeTable()` method catches the exception and returns empty columns/metadata. The table is then synced as an empty record with `tableType=UNKNOWN` and `provider=UNKNOWN` — it is **not flagged as a failure**.

```kotlin
// LakehouseMetadataExtractor.kt:461-472
private fun describeTable(catalog: String, schema: String, tableName: String): TableDescription {
    var rawColumns: List<Row> = listOf()
    try {
        rawColumns = spark.sql("describe extended `$catalog`.`$schema`.`$tableName`").collectAsList()
    } catch (th: Throwable) {
        logger.warn("Couldn't describeTable for {}.{}.{}", catalog, schema, tableName, th)
    }
    return processTableColumns(rawColumns)
}
```

## Proposed Solutions

### Option 1: Reduce S3 retries via Spark config

Add to `spark-defaults.conf` or pass at spark-submit time:

```properties
spark.hadoop.fs.s3a.retry.limit=1
spark.hadoop.fs.s3a.attempts.maximum=1
spark.hadoop.fs.s3a.connection.timeout=5000
```

**Pros:** Simple config change, no code modification.
**Cons:** Only helps with S3-specific failures. Doesn't cover timeouts from other sources (REST catalog, network issues).

### Option 2: Per-table processing timeout (recommended)

Wrap `scrapeTable` with a configurable timeout so no single table can block the pipeline indefinitely.

In Phase 2 of `LakehouseMetadataExtractor.scrape()` (line 118-149), replace the direct `scrapeTable` call with a timeout-bounded future:

```kotlin
val timeoutSeconds = ConfigProvider.getConfig()
    .getOptionalValue("TABLE_PROCESS_TIMEOUT_SECONDS", Long::class.java)
    .orElse(30L)

// Inside the parallelStream forEach:
val scrapedData = CompletableFuture.supplyAsync({
    scrapeTable(catalog = catalogName, schema = work.schema, tableName = tableName, isTemp = isTemp)
}, pool).get(timeoutSeconds, TimeUnit.SECONDS)
```

A `TimeoutException` would be caught by the existing `catch (th: Throwable)` block, logged, and counted as a failure.

**Pros:** Protects against any type of slow failure (S3, network, catalog service). Configurable per deployment.
**Cons:** Requires code change. Must choose a default timeout that doesn't kill legitimately slow large tables.

### Option 3: Both (recommended)

Apply Option 1 to reduce unnecessary S3 retries on non-retryable errors, and Option 2 as a safety net for any unexpected slowness regardless of source.

## Recommendation

Implement **Option 3** (both). The S3 config change is low-risk and immediately reduces wasted time on 403 errors. The per-table timeout provides a general-purpose safeguard for the job as a whole.

Suggested default timeout: **30 seconds** — long enough for large Iceberg tables with many snapshots, short enough to prevent a single bad table from stalling the pipeline.