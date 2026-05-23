package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.Timestamp
import java.time.Instant
import org.jboss.logging.Logger

@ApplicationScoped
class CleanupAuditTableService {
    private val logger = Logger.getLogger(CleanupAuditTableService::class.java)

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
          end_time
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
          ${sqlTimestamp(record.endTime)} AS end_time
        """.trimIndent()

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

    private fun sqlString(value: String): String =
        "'${value.replace("\\", "\\\\").replace("'", "''")}'"

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

    private fun sqlTimestamp(value: Instant): String =
        "TIMESTAMP '${Timestamp.from(value)}'"

    private fun sqlNullableTimestamp(value: Instant?): String =
        value?.let { sqlTimestamp(it) } ?: "CAST(NULL AS TIMESTAMP)"

    private fun sqlNullableLong(value: Long?): String =
        value?.let { "${it}L" } ?: "CAST(NULL AS BIGINT)"

    companion object {
        const val AUDIT_TABLE_NAME =
            "spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs"

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
        )
    }
}
