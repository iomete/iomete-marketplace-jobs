package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@ApplicationScoped
class CleanupAuditTableService {
    private val logger = SafeLogger.getLogger(CleanupAuditTableService::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    fun ensureAuditTableExists() {
        logger.info("Ensuring cleanup audit table exists: $AUDIT_TABLE_NAME")

        val spark = sparkSessionProvider.getOrCreate()

        spark.sql(buildCreateTableSql())

        ensureAuditTableSchemaIsCurrent()
    }

    private fun buildCreateTableSql(): String {
        val columnDdl =
            AUDIT_TABLE_COLUMNS.joinToString(separator = ",\n              ") { (name, type) ->
                "$name $type"
            }

        return """
            CREATE TABLE IF NOT EXISTS $AUDIT_TABLE_NAME (
              $columnDdl
            )
            USING iceberg
            PARTITIONED BY (days(start_time))
            """.trimIndent()
    }

    private fun ensureAuditTableSchemaIsCurrent() {
        val spark = sparkSessionProvider.getOrCreate()
        val existingColumns = spark.table(AUDIT_TABLE_NAME).schema().fieldNames().map { it.lowercase() }.toSet()
        AUDIT_TABLE_COLUMNS.forEach { (name, type) ->
            if (name !in existingColumns) {
                logger.info("Adding missing cleanup audit column: $name $type")
                spark.sql("ALTER TABLE $AUDIT_TABLE_NAME ADD COLUMN $name $type")
            }
        }
    }

    fun writeAuditRecord(record: CleanupAuditRecord) {
        logger.info(
            "Writing cleanup audit record for runId=${record.runId}, catalog=${record.catalogName}, database=${record.databaseName}, status=${record.status}"
        )
        sparkSessionProvider.getOrCreate().sql(buildInsertAuditRecordSql(record))
    }

    internal fun buildInsertAuditRecordSql(record: CleanupAuditRecord): String =
        """
        INSERT INTO $AUDIT_TABLE_NAME (
          run_id,
          spark_app_id,
          runtime_compute_id,
          runtime_compute_namespace,
          runtime_domain,
          runtime_user,
          external_job_id,
          platform_started_by,
          catalog_name,
          database_name,
          operation,
          dry_run,
          delete_enabled,
          older_than_hours,
          cutoff_time,
          max_candidate_folders_per_database,
          excluded_paths,
          status,
          status_reason,
          error_message,
          discovered_database_location,
          storage_scan_location,
          active_table_count,
          unresolved_table_count,
          potentially_untracked_folder_count,
          storage_folder_count,
          candidate_folder_count,
          candidate_object_count,
          candidate_total_size_bytes,
          deleted_folder_count,
          deleted_object_count,
          deleted_total_size_bytes,
          candidate_folders,
          deleted_folders,
          diagnostic_details,
          start_time,
          end_time,
          candidate_fingerprints,
          prev_run_id
        )
        SELECT
          ${sqlString(record.runId)} AS run_id,
          ${sqlNullableString(record.sparkAppId)} AS spark_app_id,
          ${sqlNullableString(record.runtimeComputeId)} AS runtime_compute_id,
          ${sqlNullableString(record.runtimeComputeNamespace)} AS runtime_compute_namespace,
          ${sqlNullableString(record.runtimeDomain)} AS runtime_domain,
          ${sqlNullableString(record.runtimeUser)} AS runtime_user,
          ${sqlNullableString(record.externalJobId)} AS external_job_id,
          ${sqlNullableString(record.platformStartedBy)} AS platform_started_by,
          ${sqlString(record.catalogName)} AS catalog_name,
          ${sqlString(record.databaseName)} AS database_name,
          ${sqlString(record.operation)} AS operation,
          ${record.dryRun} AS dry_run,
          ${record.deleteEnabled} AS delete_enabled,
          ${record.olderThanHours}L AS older_than_hours,
          ${sqlNullableTimestamp(record.cutoffTime)} AS cutoff_time,
          ${record.maxCandidateFoldersPerDatabase} AS max_candidate_folders_per_database,
          ${sqlStringArray(record.excludedPaths)} AS excluded_paths,
          ${sqlString(record.status)} AS status,
          ${sqlNullableString(record.statusReason)} AS status_reason,
          ${sqlNullableString(record.errorMessage)} AS error_message,
          ${sqlNullableString(record.discoveredDatabaseLocation)} AS discovered_database_location,
          ${sqlString(record.storageScanLocation)} AS storage_scan_location,
          ${record.activeTableCount}L AS active_table_count,
          ${record.unresolvedTableCount}L AS unresolved_table_count,
          ${record.potentiallyUntrackedFolderCount}L AS potentially_untracked_folder_count,
          ${record.storageFolderCount}L AS storage_folder_count,
          ${record.candidateFolderCount}L AS candidate_folder_count,
          ${sqlNullableLong(record.candidateObjectCount)} AS candidate_object_count,
          ${sqlNullableLong(record.candidateTotalSizeBytes)} AS candidate_total_size_bytes,
          ${record.deletedFolderCount}L AS deleted_folder_count,
          ${sqlNullableLong(record.deletedObjectCount)} AS deleted_object_count,
          ${sqlNullableLong(record.deletedTotalSizeBytes)} AS deleted_total_size_bytes,
          ${sqlStringArray(record.candidateFolders)} AS candidate_folders,
          ${sqlStringArray(record.deletedFolders)} AS deleted_folders,
          ${sqlStringMap(record.diagnosticDetails)} AS diagnostic_details,
          ${sqlTimestamp(record.startTime)} AS start_time,
          ${sqlTimestamp(record.endTime)} AS end_time,
          ${record.candidateFingerprints?.let(::sqlStringMap) ?: "CAST(NULL AS MAP<STRING, STRING>)"} AS candidate_fingerprints,
          ${sqlNullableString(record.prevRunId)} AS prev_run_id
        """.trimIndent()

    /**
     * Earlier rows of one job, catalog and database that started at or after [since]. Values are compared as stored,
     * so callers pass them made safe as the recorder writes them. Throws [AuditHistoryUnavailableException] when the
     * table is not one whose rows can be trusted to be every committed row.
     */
    fun readHistory(
        externalJobId: String,
        catalogName: String,
        databaseName: String,
        since: Instant,
    ): List<AuditHistoryRow> {
        val spark = sparkSessionProvider.getOrCreate()
        val problem =
            try {
                historyTrustProblem(
                    describeRows = spark.sql("DESCRIBE TABLE EXTENDED $AUDIT_TABLE_NAME").collectAsList(),
                    schema = spark.table(AUDIT_TABLE_NAME).schema(),
                    properties = spark.sql("SHOW TBLPROPERTIES $AUDIT_TABLE_NAME").collectAsList().associate { it.getString(0) to it.getString(1) },
                    sessionWapSet = WAP_SESSION_KEYS.any { spark.conf().getOption(it).isDefined },
                )
            } catch (th: Exception) {
                throw AuditHistoryUnavailableException("could not describe the audit table", th)
            }
        problem?.let { throw AuditHistoryUnavailableException(it) }

        return try {
            spark.table(AUDIT_TABLE_NAME)
                .where(
                    sameJob(externalJobId, catalogName, databaseName)
                        .and(col("start_time").geq(lit(Timestamp.from(since)))),
                ).select(*HISTORY_COLUMNS.map { col(it.first) }.toTypedArray())
                .collectAsList()
                .map(::historyRow)
        } catch (th: Exception) {
            throw AuditHistoryUnavailableException("could not read the audit history", th)
        }
    }

    /** Whether the deletion intent row of [runId] reads back. */
    fun intentRecorded(
        runId: String,
        externalJobId: String,
        catalogName: String,
        databaseName: String,
    ): Boolean =
        sparkSessionProvider.getOrCreate().table(AUDIT_TABLE_NAME)
            .where(
                sameJob(externalJobId, catalogName, databaseName)
                    .and(col("run_id").equalTo(lit(runId)))
                    .and(col("status").equalTo(lit(AUDIT_STATUS_INTENT))),
            ).count() == 1L

    private fun sameJob(
        externalJobId: String,
        catalogName: String,
        databaseName: String,
    ): Column =
        col("external_job_id").equalTo(lit(externalJobId))
            .and(col("catalog_name").equalTo(lit(catalogName)))
            .and(col("database_name").equalTo(lit(databaseName)))

    fun currentSparkAppId(): String? =
        sparkSessionProvider.getOrCreate().sparkContext().applicationId()

    fun currentRuntimeComputeId(): String? =
        env("IOMETE_COMPUTE_ID")

    fun currentRuntimeComputeNamespace(): String? =
        env("IOMETE_COMPUTE_NAMESPACE")

    fun currentRuntimeDomain(): String? =
        env("IOMETE_DOMAIN")

    fun currentRuntimeUser(): String? =
        env("SPARK_USER")

    fun currentExternalJobId(): String? =
        env("IOMETE_EXTERNAL_JOB_ID")

    fun currentPlatformStartedBy(): String? =
        env("IOMETE_JOB_STARTED_BY")

    fun logRuntimeIdentityEnvVars() {
        val identityEnvVars = listOf(
            "IOMETE_COMPUTE_ID" to currentRuntimeComputeId(),
            "IOMETE_COMPUTE_NAMESPACE" to currentRuntimeComputeNamespace(),
            "IOMETE_DOMAIN" to currentRuntimeDomain(),
            "SPARK_USER" to currentRuntimeUser(),
            "IOMETE_EXTERNAL_JOB_ID" to currentExternalJobId(),
            "IOMETE_JOB_STARTED_BY" to currentPlatformStartedBy(),
        )

        val summary = identityEnvVars.joinToString(", ") { (name, value) ->
            if (value == null) "$name=unset" else "$name=set(len=${value.length})"
        }

        logger.info("Runtime identity env vars: $summary")
    }

    private fun env(name: String): String? =
        System.getenv(name)?.takeIf { it.isNotBlank() }

    private fun sqlNullableString(value: String?): String =
        value?.let { sqlString(it) } ?: "CAST(NULL AS STRING)"

    // Spark has no '' escape: it reads two literals and joins them, which drops the quote. So a backslash is written
    // as \\ and a quote as \'. A dollar sign is written as \u0024, so no ${...} substitution in any parser of the chain
    // can see one. An asterisk is written as \u002A, so the text holds no '/*' for the comment pattern the Iceberg
    // parser runs over the whole statement, which is slow on an unclosed one. Control, format and separator
    // characters, and unpaired surrogates, are written as \uXXXX, so the statement text, wherever Spark or a plugin
    // logs it, holds none of them raw.
    private fun sqlString(value: String): String =
        buildString(value.length + 2) {
            append('\'')
            var index = 0
            while (index < value.length) {
                val codePoint = value.codePointAt(index)
                val end = index + Character.charCount(codePoint)
                when {
                    codePoint == '\\'.code -> append("\\\\")
                    codePoint == '\''.code -> append("\\'")
                    codePoint == '$'.code || codePoint == '*'.code || isHidden(codePoint) ->
                        (index until end).forEach { append("\\u%04X".format(value[it].code)) }
                    else -> append(value, index, end)
                }
                index = end
            }
            append('\'')
        }

    private fun isHidden(codePoint: Int): Boolean =
        when (Character.getType(codePoint).toByte()) {
            Character.CONTROL, Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR, Character.SURROGATE -> true
            else -> false
        }

    private fun sqlStringArray(values: List<String>): String =
        if (values.isEmpty()) {
            "CAST(ARRAY() AS ARRAY<STRING>)"
        } else {
            values.joinToString(
                prefix = "ARRAY(",
                postfix = ")",
                transform = ::sqlString,
            )
        }

    private fun sqlStringMap(values: Map<String, String>): String =
        if (values.isEmpty()) {
            "CAST(MAP() AS MAP<STRING, STRING>)"
        } else {
            values.entries.joinToString(
                prefix = "MAP(",
                postfix = ")",
            ) { entry ->
                "${sqlString(entry.key)}, ${sqlString(entry.value)}"
            }
        }

    // The zone is part of the text, so the stored instant depends on neither the JVM nor the session time zone.
    private fun sqlTimestamp(value: Instant): String =
        "TIMESTAMP '${AUDIT_TIMESTAMP_FORMAT.format(value)}'"

    private fun sqlNullableTimestamp(value: Instant?): String =
        value?.let { sqlTimestamp(it) } ?: "CAST(NULL AS TIMESTAMP)"

    private fun sqlNullableLong(value: Long?): String =
        value?.let { "${it}L" } ?: "CAST(NULL AS BIGINT)"

    companion object {
        // Write-audit-publish stages a write as an unpublished snapshot, so a row the job wrote may not be read back.
        private val WAP_SESSION_KEYS = listOf("spark.wap.id", "spark.wap.branch")
        private const val WAP_TABLE_PROPERTY = "write.wap.enabled"

        // The columns confirmation reads, with the type each must have. A type is compared without nullability.
        private val HISTORY_COLUMNS: List<Pair<String, String>> =
            listOf(
                "run_id" to "string",
                "status" to "string",
                "end_time" to "timestamp",
                "candidate_folders" to "array<string>",
                "deleted_folders" to "array<string>",
                "candidate_fingerprints" to "map<string,string>",
                "prev_run_id" to "string",
            )
        private val FILTER_COLUMNS: List<Pair<String, String>> =
            listOf(
                "external_job_id" to "string",
                "catalog_name" to "string",
                "database_name" to "string",
                "start_time" to "timestamp",
            )

        /** Why the audit table's rows cannot be trusted for confirmation, or null when they can. */
        internal fun historyTrustProblem(
            describeRows: List<Row>,
            schema: StructType,
            properties: Map<String, String?>,
            sessionWapSet: Boolean,
        ): String? {
            val provider =
                describeRows.singleOrNull { it.getString(0)?.trim().equals("Provider", ignoreCase = true) }?.getString(1)?.trim()
            if (!provider.equals("iceberg", ignoreCase = true)) {
                return "the audit table is not an Iceberg table"
            }
            (HISTORY_COLUMNS + FILTER_COLUMNS).forEach { (name, type) ->
                val fields = schema.fields().filter { it.name().equals(name, ignoreCase = true) }
                if (fields.size != 1 || fields.single().dataType().simpleString() != type) {
                    return "audit column $name is missing or not of type $type"
                }
            }
            if (properties[WAP_TABLE_PROPERTY]?.trim().equals("true", ignoreCase = true)) {
                return "write-audit-publish is enabled on the audit table"
            }
            if (sessionWapSet) {
                return "the session sets a write-audit-publish id or branch"
            }
            return null
        }

        internal fun historyRow(row: Row): AuditHistoryRow {
            fun string(name: String): String? = row.fieldIndex(name).let { if (row.isNullAt(it)) null else row.getString(it) }

            fun strings(name: String): List<String>? =
                row.fieldIndex(name).let { index -> if (row.isNullAt(index)) null else row.getList<String?>(index).takeIf { null !in it }?.map { it!! } }

            val fingerprintIndex = row.fieldIndex("candidate_fingerprints")
            val fingerprints =
                if (row.isNullAt(fingerprintIndex)) {
                    null
                } else {
                    row.getJavaMap<String?, String?>(fingerprintIndex).entries
                        .takeIf { entries -> entries.all { it.key != null && it.value != null } }
                        ?.associate { it.key!! to it.value!! }
                }
            val endIndex = row.fieldIndex("end_time")
            val endTime =
                if (row.isNullAt(endIndex)) {
                    null
                } else {
                    when (val value = row.get(endIndex)) {
                        is Timestamp -> value.toInstant()
                        is Instant -> value
                        else -> null
                    }
                }
            return AuditHistoryRow(
                runId = string("run_id"),
                status = string("status"),
                endTime = endTime,
                candidateFolders = strings("candidate_folders"),
                deletedFolders = strings("deleted_folders"),
                candidateFingerprints = fingerprints,
                prevRunId = string("prev_run_id"),
            )
        }

        const val AUDIT_TABLE_NAME =
            "spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs"

        // Microseconds, the precision Spark stores; the rest is cut, as Spark cuts it.
        private val AUDIT_TIMESTAMP_FORMAT: DateTimeFormatter =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSXXX").withZone(ZoneOffset.UTC)

        private val AUDIT_TABLE_COLUMNS: List<Pair<String, String>> = listOf(
            "run_id" to "STRING",
            "spark_app_id" to "STRING",
            "runtime_compute_id" to "STRING",
            "runtime_compute_namespace" to "STRING",
            "runtime_domain" to "STRING",
            "runtime_user" to "STRING",
            "external_job_id" to "STRING",
            "platform_started_by" to "STRING",
            "catalog_name" to "STRING",
            "database_name" to "STRING",
            "operation" to "STRING",
            "dry_run" to "BOOLEAN",
            "delete_enabled" to "BOOLEAN",
            "older_than_hours" to "BIGINT",
            "cutoff_time" to "TIMESTAMP",
            "max_candidate_folders_per_database" to "INT",
            "excluded_paths" to "ARRAY<STRING>",
            "status" to "STRING",
            "status_reason" to "STRING",
            "error_message" to "STRING",
            "discovered_database_location" to "STRING",
            "storage_scan_location" to "STRING",
            "active_table_count" to "BIGINT",
            "unresolved_table_count" to "BIGINT",
            "potentially_untracked_folder_count" to "BIGINT",
            "storage_folder_count" to "BIGINT",
            "candidate_folder_count" to "BIGINT",
            "candidate_object_count" to "BIGINT",
            "candidate_total_size_bytes" to "BIGINT",
            "deleted_folder_count" to "BIGINT",
            "deleted_object_count" to "BIGINT",
            "deleted_total_size_bytes" to "BIGINT",
            "candidate_folders" to "ARRAY<STRING>",
            "deleted_folders" to "ARRAY<STRING>",
            "diagnostic_details" to "MAP<STRING, STRING>",
            "start_time" to "TIMESTAMP",
            "end_time" to "TIMESTAMP",
            "candidate_fingerprints" to "MAP<STRING, STRING>",
            "prev_run_id" to "STRING",
        )
    }
}
