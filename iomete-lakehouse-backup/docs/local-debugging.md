# Local Debugging with the Exact Production Spark

## Purpose

This guide explains how to run `com.iomete.backup.App` on your machine under a
debugger, using the **exact Spark runtime that ships in the production image**
rather than a stock Apache Spark from Maven. Breakpoints, variable inspection,
and step-through all work, and the job executes against the same custom
IOMETE-patched Spark, Hadoop, and AWS SDK jars that run in production — so what
you observe locally matches the deployed behaviour.

The approach: extract `/opt/spark` out of the base image, launch its
`spark-submit` on your host with a JDWP debug agent, and attach IntelliJ as a
remote debugger.

## Why not just run `App.main` directly in IntelliJ?

Two friction points make the "exact Spark" route preferable:

1. **The runtime Spark/Hadoop jars are `compileOnly`.** They are provided by the
   base image at runtime, so they are absent from the `main` module's runtime
   classpath. Running `App.main` directly would force you to borrow the `test`
   module's classpath, which pulls **Maven's stock Spark 3.5.7**, not the
   IOMETE-patched build.
2. **No Spark master is configured in code.** `SparkSessionProvider` calls
   `SparkSession.builder().orCreate` with no master, expecting `spark-submit` to
   supply it.

Extracting the image's Spark and submitting through it resolves both: you get the
real jars, and `spark-submit` provides the master and the JDK-17 module options.

A lighter-weight alternative (stock Maven Spark, run `App.main` from the `test`
module classpath with `-Dspark.master=local[*]`) is documented at the end for
quick logic-only debugging where exact-parity is not required.

## Prerequisites

- **JDK 17** installed locally. Spark 3.5.x targets Java 17, and the image's JVM
  is `17.0.x`.
- **Docker**, only to extract Spark from the image (not at runtime). You must be
  able to pull or already have `iomete.azurecr.io/iomete/spark:<tag>` locally;
  authenticate with `docker login iomete.azurecr.io` if needed.
- **IntelliJ IDEA** with this project imported.
- **An S3 source and target** you can read/write for the test copy, with
  credentials.

The image tag is derived from `gradle.properties`
(`sparkVersion` + `sparkImageRevision`), matching the `FROM` line in the
`Dockerfile`. At the time of writing that is `spark:3.5.7-v3`; use whatever the
current values are.

## One-time setup

### 1. Extract the exact Spark distribution from the image

```bash
docker create --name spark-extract iomete.azurecr.io/iomete/spark:3.5.7-v3
docker cp spark-extract:/opt/spark ./.local-spark
docker rm spark-extract
```

This produces `./.local-spark`, a complete Spark distribution containing the
IOMETE-patched `spark-sql`, `hadoop-*` (3.4.1), and the AWS SDK v2 `bundle`
(2.24.6) needed for the S3A filesystem. `.local-spark/` is gitignored.

`/opt/spark` is pure JVM jars plus shell launchers, so it runs on macOS and Linux
alike. The bundled native codecs (snappy, zstd) are cross-platform. The only
thing you do not reproduce locally is the Linux userland, which is irrelevant for
this job.

### 2. Create a local config file

The application takes a single argument: the path to a JSON config (default
`/etc/configs/application.json`). For local runs, create
`conf/application.local.json`. This filename is **gitignored** specifically
because it holds real credentials — never commit it, and never put real keys in
any tracked file.

> **Note on `${VAR}` placeholders.** The `README` and production job config use
> `${SOURCE_ACCESS_KEY}`-style placeholders. Those are resolved by the IOMETE
> platform when it renders the job config; the application itself performs **no**
> environment-variable substitution. A local config must therefore contain
> literal values.

Example for AWS S3 (adjust to your buckets, region, and keys):

```json
{
  "source": {
    "type": "s3",
    "bucket": "my-source-bucket",
    "prefix": "path/to/data",
    "pathStyleAccess": false,
    "region": "us-east-1",
    "accessKey": "AKIAEXAMPLE",
    "secretKey": "REDACTED"
  },
  "target": {
    "type": "s3",
    "bucket": "my-target-bucket",
    "prefix": "backup/local-test",
    "pathStyleAccess": false,
    "region": "us-east-1",
    "accessKey": "AKIAEXAMPLE",
    "secretKey": "REDACTED"
  }
}
```

For an S3-compatible endpoint (e.g. Isilon/OneFS S3, or a non-AWS gateway), set
`"endpoint"` and usually `"pathStyleAccess": true`.

Start with a small source prefix. The job enumerates and copies every object
under the prefix; point it at something cheap for the first run.

## The debug loop

### 3. Build the application jar

```bash
./gradlew jar
```

This produces `build/libs/iomete-lakehouse-backup-<version>.jar` (the version
comes from `projectVersion` in `gradle.properties`). Rebuild this whenever you
change source, so the jar the debugger runs matches the sources IntelliJ shows.

### 4. Launch under the debug agent

```bash
SPARK_LOCAL_IP=127.0.0.1 JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
./.local-spark/bin/spark-submit \
  --master 'local[*]' \
  --driver-java-options '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005' \
  --class com.iomete.backup.App \
  build/libs/iomete-lakehouse-backup-1.0.0.jar \
  conf/application.local.json
```

Notes:

- `suspend=y` makes the JVM wait until the debugger attaches, so you never miss
  early breakpoints (config parsing, session init). Change to `suspend=n` if you
  prefer it to start immediately.
- `--master 'local[*]'` supplies the master the code omits.
- The required JDK-17 `--add-opens` flags are added **automatically** by Spark's
  launcher (`JavaModuleOptions`); you do not need to pass them. This is why the
  image's `spark-defaults.conf` contains none.
- On Linux, replace the `JAVA_HOME` expression with your JDK 17 path.
- Update the jar filename if `projectVersion` changes.

The process prints the Spark banner and then blocks on
`Listening for transport dt_socket at address: 5005`.

### 5. Attach IntelliJ

1. `Run → Edit Configurations → + → Remote JVM Debug`.
2. Name it e.g. `Attach: local spark-submit`; host `localhost`, port `5005`;
   leave the defaults otherwise.
3. Set breakpoints — good starting points are `App.run` (`App.kt`),
   `HadoopConfigBuilder.buildConfigMap`, `FileLister.listRecursively`, and
   `CopyJobRunner.run`.
4. Click **Debug**. Execution resumes into your breakpoints, running on the exact
   image Spark.

### Iterating

After a code change: **Stop** the run, `./gradlew jar`, re-run step 4, re-attach
(step 5). Keep the Remote JVM Debug configuration; you only recreate it once.

## Inspecting execution with the Spark UI

The Spark UI shows jobs, stages, tasks, the DAG, executor/storage state, and the
full environment — useful for confirming how the copy is partitioned and where
time goes.

### While a run is in progress (live UI)

When the driver starts it serves the UI at **`http://localhost:4040`** (if 4040
is taken it uses 4041, 4042, … — check the launch log for
`Successfully started service 'SparkUI' on port <n>`). Open it in a browser while
the job runs.

The UI is bound to the driver's lifetime and disappears the moment the
`SparkContext` stops — and this job stops the session in a `finally` block as
soon as the copy finishes, so a fast run gives you almost no time to look. Two
ways to keep it up while debugging:

- Launch with `suspend=y` (step 4) and set a breakpoint; while the JVM is paused
  at a breakpoint the driver — and therefore the UI — stays alive and browsable.
- Put a breakpoint on the `SparkSessionProvider.stop()` call near the end of
  `App.run` so the context is still running when you inspect the UI.

### After a run has finished (History Server)

To inspect a completed run, record its event log and replay it with the History
Server. Event logging is **off by default** in the extracted distribution, so
enable it per run.

1. Create the event-log directory once (Spark does not create it and will fail if
   it is missing):

   ```bash
   mkdir -p /tmp/spark-events
   ```

2. Add the event-log flags to the step-4 launch command:

   ```bash
   ... ./.local-spark/bin/spark-submit \
     --master 'local[*]' \
     --conf spark.eventLog.enabled=true \
     --conf spark.eventLog.dir=file:///tmp/spark-events \
     --driver-java-options '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005' \
     --class com.iomete.backup.App \
     build/libs/iomete-lakehouse-backup-1.0.0.jar \
     conf/application.local.json
   ```

3. Start the History Server against that directory (it reads
   `spark.history.fs.logDirectory`):

   ```bash
   JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
   SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///tmp/spark-events" \
   ./.local-spark/sbin/start-history-server.sh
   ```

   Open **`http://localhost:18080`** and select the run. Completed runs remain
   available across restarts as long as their event logs stay in the directory.

4. Stop it when done:

   ```bash
   ./.local-spark/sbin/stop-history-server.sh
   ```

## Alternative: quick logic-only debugging (stock Maven Spark)

When you only need to step through application logic (config parsing, path
resolution, listing) and do not need production-exact Spark internals, you can
skip the extraction and run `App.main` straight from IntelliJ:

1. `Run → Edit Configurations → + → Application`.
2. Main class `com.iomete.backup.App`; program arguments
   `conf/application.local.json`.
3. **Use classpath of module:** the project's **`.test`** module — the `.main`
   module lacks Spark because it is `compileOnly`, whereas `.test` has it via
   `testImplementation`.
4. VM options: `-Dspark.master=local[*]`.
5. Environment: `SPARK_LOCAL_IP=127.0.0.1`.
6. **Debug**.

This uses Maven's stock Spark 3.5.7, not the IOMETE build, so treat findings
about Spark/Hadoop internals with caution. For anything sensitive to the real
runtime, use the exact-Spark route above.

## Troubleshooting

- **`InaccessibleObjectException` / module access errors:** you are likely not
  going through `spark-submit`. Launch via `.local-spark/bin/spark-submit` so the
  launcher injects the `--add-opens` flags, or add them manually to
  `--driver-java-options`.
- **`Address already in use` on 5005:** a previous run is still attached or
  lingering. Stop it, or change the port in both the launch command and the
  Remote JVM Debug configuration.
- **S3 `403`/`AccessDenied` or `UnknownHost`:** re-check `accessKey`/`secretKey`,
  `region`, and (for non-AWS) `endpoint` and `pathStyleAccess` in
  `conf/application.local.json`.
- **Breakpoints not hit / greyed out:** the running jar is stale. Rebuild with
  `./gradlew jar` before relaunching so bytecode and sources line up.
