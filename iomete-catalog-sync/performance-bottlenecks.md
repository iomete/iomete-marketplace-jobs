# Catalog Sync Performance Bottlenecks & Fixes

## 1. Three Sequential Snapshot Queries Per Iceberg Table

**Impact: High**

**Evidence:** `IcebergTableExtractor.kt:26-68` — for every Iceberg table, three separate Spark SQL queries are fired sequentially:

1. **Line 28-38:** `SELECT ... FROM snapshots ORDER BY committed_at DESC LIMIT 1` (last snapshot)
2. **Line 42-53:** `SELECT ... FROM snapshots ORDER BY committed_at ASC LIMIT 1` (first snapshot)
3. **Line 57-68:** `SELECT ... FROM snapshots WHERE snapshot_id != $firstSnapshotId` with `SUM(added-data-files)`, `SUM(added-files-size)` (all other snapshots)

**Why it's slow:** Each query triggers a full Spark job against the Iceberg metadata. For tables with many snapshots, the third query scans every snapshot row. Queries 2 and 3 are also sequentially dependent (query 3 needs `firstSnapshotId` from query 2). Multiply by every Iceberg table and this becomes the dominant cost.

**Fix:** Collapse all three into a single query. The snapshots metadata table is small (one row per snapshot), so collect once and compute first/last/aggregates in Kotlin on the driver side. Alternatively, combine into a single SQL using window functions.

---

## 2. Sequential Catalog & Schema Processing

**Impact: High**

**Evidence:** `MetadataScraper.kt:42` — `.onEach { catalog -> ... }` processes catalogs one at a time. Inside each catalog, `MetadataScraper.kt:48` — `.asSequence().mapNotNull { ... }` processes schemas one at a time.

**Why it's slow:** If there are 5 catalogs with 20 schemas each, that's 100 schemas processed serially. A single slow remote catalog (Glue, REST-based Nessie) blocks everything behind it.

**Fix:** Parallelize catalog processing (each already gets its own SparkSession via the `sessions` cache at `App.kt:79`). Schema processing within a catalog can also be parallelized since each schema's work is independent.

---

## 3. Per-Column Sequential Presidio HTTP Calls

**Impact: Medium-High**

**Evidence:** `PIIDetectionService.kt:36` — `columns.forEach { columnName -> ... }` iterates every column sequentially, and at line 66, `presidioClient.analyze(PresidioRequest(input))` makes a blocking HTTP call for each column.

**Why it's slow:** A table with 50 columns means 50 sequential HTTP round-trips to Presidio. With network latency, even 20ms per call adds up to 1 second per table. Across hundreds of tables, this becomes significant.

**Fix:** Parallelize the Presidio calls (e.g., using coroutines or a thread pool), or batch multiple column samples into a single Presidio request if the API supports it.

---

## 4. `parallelStream` Uses a Shared ForkJoinPool with Blocking I/O

**Impact: Medium**

**Evidence:** `MetadataScraper.kt:86` — `.parallelStream()` uses Java's common `ForkJoinPool`, which defaults to `Runtime.availableProcessors() - 1` threads.

**Why it's slow:** Every table in the parallel stream does blocking I/O (Spark SQL queries, HTTP calls to Presidio, HTTP calls to catalog service). Blocking I/O on a ForkJoinPool means threads sit idle waiting for responses, severely limiting actual parallelism. With 4 cores, only ~3 tables are processed concurrently, even though the work is I/O-bound, not CPU-bound.

**Fix:** Use a custom thread pool with a higher thread count suited for I/O-bound work (e.g., `Executors.newFixedThreadPool(20)`), or use Kotlin coroutines with a dispatcher tuned for I/O.

---

## 5. Per-Table Blocking HTTP Index Call

**Impact: Medium**

**Evidence:** `MetadataScraper.kt:100` — `catalogServiceClient.indexTable(it)` is called once per table inside the `parallelStream`. This is a blocking `POST` per table.

**Why it's slow:** The HTTP call adds latency to each table's processing time. The parallel stream's thread is blocked waiting for the HTTP response before it can pick up the next table. With hundreds or thousands of tables, this serializes a significant amount of network I/O.

**Fix:** Decouple indexing from extraction. Collect extracted metadata and batch-index them (e.g., a bulk endpoint), or fire the HTTP calls asynchronously so they don't block the parallel stream threads.

---

## 6. `TABLESAMPLE` Triggers Actual Data Reading for PII Detection

**Impact: Medium**

**Evidence:** `PIIDetectionService.kt:32` — `SELECT * FROM $fullTableName TABLESAMPLE (5 ROWS)` runs for every table that supports column tags (all Iceberg, Parquet/ORC, and View tables per the extractor factory).

**Why it's slow:** `TABLESAMPLE` with row-based sampling in Spark needs to open data files, read rows, and collect them to the driver. For wide tables or tables stored remotely (S3/ADLS), this incurs file open + I/O overhead per table.

**Fix:** If PII detection is enabled, consider limiting PII scanning to only new/changed tables (skip tables whose `lastModified` hasn't changed since last sync), or sample in batch across multiple tables.

---

## Summary

| # | Bottleneck | Location | Impact |
|---|-----------|----------|--------|
| 1 | 3 sequential snapshot queries per Iceberg table | `IcebergTableExtractor.kt:26-68` | **High** |
| 2 | Sequential catalogs & schemas | `MetadataScraper.kt:42,48` | **High** |
| 3 | Per-column sequential Presidio calls | `PIIDetectionService.kt:36,66` | **Medium-High** |
| 4 | `parallelStream` on shared ForkJoinPool with blocking I/O | `MetadataScraper.kt:86` | **Medium** |
| 5 | Per-table blocking HTTP index call | `MetadataScraper.kt:100` | **Medium** |
| 6 | `TABLESAMPLE` per table for PII | `PIIDetectionService.kt:32` | **Medium** |
