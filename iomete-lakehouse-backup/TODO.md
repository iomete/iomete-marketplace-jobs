# Lakehouse Backup - Post-POC TODO

Issues and considerations to address once the POC implementation is complete.

## Cross-Filesystem Incremental Copy

- [ ] **mtime comparison is unreliable across filesystems** — When copying HDFS→S3, the S3 object's `LastModified` becomes the upload time, not the original HDFS mtime. A second run would see different mtimes and re-copy everything. Options:
  - Use size-only comparison as the default cross-filesystem strategy
  - Store original mtime as S3 object metadata (`x-amz-meta-original-mtime`)
  - Use a manifest/journal file to track what was previously copied

- [ ] **Checksum comparison won't work across S3 and HDFS** — S3 uses MD5/ETag, HDFS uses CRC32C. `FileSystem.getFileChecksum()` returns incompatible types for different filesystems. Checksum-based incremental only works within the same filesystem type (S3→S3 or HDFS→HDFS). Document this limitation or remove cross-filesystem checksum as an option.

## S3/ECS Configuration

- [x] **Same-bucket S3/ECS source and target now use isolated S3A clients** — When source and target share the same bucket name but use different endpoints/credentials, per-bucket overrides (`fs.s3a.bucket.BUCKETNAME.*`) are not enough because both sides map to the same literal bucket key. Keep source and target in separate Hadoop `Configuration` objects and open isolated `FileSystem.newInstance(...)` clients so cached S3A state does not bleed between sides. Per-bucket overrides remain useful only when bucket names differ.

- [ ] **ECS-specific S3A settings** — Dell ECS may require:
  - `fs.s3a.change.detection.mode=none` (ECS may not support ETag-based change detection)
  - `fs.s3a.endpoint.region=us-east-1` (any non-empty value, required by S3A SigV4)

## Scalability

- [ ] **Driver-side file listing bottleneck** — `FileLister` runs on the driver with a single `listFiles()` call. For directories with millions of files this will be slow and memory-constrained. Scaling path: list top-level dirs on driver, distribute sub-directory listing to executors.

## Bugs (Runtime Failures)

- [ ] **`HADOOP_USER_NAME` set as config property has no effect** — `HadoopConfigBuilder` sets `props["HADOOP_USER_NAME"] = auth.user`, but `HADOOP_USER_NAME` is an environment variable, not a Hadoop Configuration property. `UserGroupInformation` reads it from `System.getenv()`, not from `Configuration`. Fix: use `UserGroupInformation.createRemoteUser(user)` + `ugi.doAs()`, or `System.setProperty("HADOOP_USER_NAME", user)` before UGI initialization.

- [ ] **Kerberos auth sets config but never performs login** — `HadoopConfigBuilder` sets `hadoop.security.authentication=kerberos` and keytab properties, but never calls `UserGroupInformation.loginUserFromKeytab(principal, keytabPath)`. The config alone tells Hadoop to *expect* Kerberos but doesn't authenticate. Any HDFS operation will fail with an auth error.

## Performance

- [ ] **`FileUtil.copy()` streams all data through executors** — For S3→S3 copies, data is read from source into executor memory and written back to target, instead of using S3's server-side `CopyObject` API. This is a major bottleneck for large data volumes. Cross-filesystem copies (S3→HDFS) have no alternative, but same-filesystem S3→S3 should use native copy.

- [ ] **FileSystem and Configuration recreated per file on executors** — `FileCopier.copySingleFile()` reconstructs `Configuration` and calls `FileSystem.get()` for every file. Use `mapPartitions` instead of `map` so the FileSystem is initialized once per partition and reused across files in that partition.

- [ ] **`CopyJobRunner.collect()` pulls all results to driver memory** — For millions of files, all `CopyResult` objects are collected to the driver. Use `treeAggregate()` to compute the summary (counts, byte totals, error list) on executors and only return the aggregated `CopyJobSummary`.

## Unimplemented Config Options

- [ ] **`bandwidthMb` is parsed but never applied** — The config accepts a bandwidth limit but the copy logic doesn't throttle.

- [ ] **`numListStatusThreads` is parsed but never used** — The listing operation doesn't use this value.

- [ ] **Incremental mode not implemented** — `CopyMode.INCREMENTAL` and `IncrementalStrategy` are parsed and validated but the copy logic always does a full copy regardless of mode.

- [ ] **Metrics JSON output not implemented** — README describes a metrics JSON file written to the target on completion, but this isn't produced.

## Code Quality

- [ ] **`FAIL_ON_UNKNOWN_PROPERTIES = false` silently ignores config typos** — A typo like `"acccessKey"` would be silently ignored, leaving `accessKey` null and causing a confusing runtime error. Consider logging a warning for unknown properties or switching to `FAIL_ON_UNKNOWN_PROPERTIES = true` since the validation layer would catch the resulting missing fields.

- [ ] **`SparkSessionProvider` race condition** — The lazy initialization has a potential race if two threads call `sparkSession` simultaneously. Use Kotlin's `lazy` delegate or `@Synchronized` for safety.

## Documentation

- [ ] **README mentions HOCON but code uses JSON** — The "Usage in IOMETE" section references `application.conf` and HOCON config, but the implementation uses `application.json` with Jackson JSON parsing. Make these consistent.