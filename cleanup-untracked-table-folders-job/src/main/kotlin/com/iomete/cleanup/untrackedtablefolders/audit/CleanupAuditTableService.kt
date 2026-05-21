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

        sparkSessionProvider.getOrCreate().sql(
            """
            CREATE TABLE IF NOT EXISTS $AUDIT_TABLE_NAME (
              run_id STRING,
              spark_app_id STRING,
              initiated_by STRING,
              catalog_name STRING,
              database_name STRING,
              operation STRING,
              dry_run BOOLEAN,
              delete_enabled BOOLEAN,
              older_than_hours BIGINT,
              cutoff_time TIMESTAMP,
              max_candidate_folders_per_database INT,
              excluded_paths ARRAY<STRING>,
              status STRING,
              status_reason STRING,
              error_message STRING,
              discovered_database_location STRING,
              storage_scan_location STRING,
              active_table_count BIGINT,
              storage_folder_count BIGINT,
              candidate_folder_count BIGINT,
              deleted_folder_count BIGINT,
              candidate_folders ARRAY<STRING>,
              deleted_folders ARRAY<STRING>,
              diagnostic_details MAP<STRING, STRING>,
              start_time TIMESTAMP,
              end_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(start_time))
            """.trimIndent()
        )
    }

    fun writeAuditRecord(record: CleanupAuditRecord) {
        logger.info(
            "Writing cleanup audit record for runId=${record.runId}, catalog=${record.catalogName}, database=${record.databaseName}, status=${record.status}"
        )

        sparkSessionProvider.getOrCreate().sql(
            """
            INSERT INTO $AUDIT_TABLE_NAME (
              run_id,
              spark_app_id,
              initiated_by,
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
              deleted_folder_count,
              candidate_folders,
              deleted_folders,
              diagnostic_details,
              start_time,
              end_time
            )
            SELECT
              ${sqlString(record.runId)} AS run_id,
              ${sqlNullableString(record.sparkAppId)} AS spark_app_id,
              ${sqlNullableString(record.initiatedBy)} AS initiated_by,
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
              ${record.deletedFolderCount}L AS deleted_folder_count,
              ${sqlStringArray(record.candidateFolders)} AS candidate_folders,
              ${sqlStringArray(record.deletedFolders)} AS deleted_folders,
              ${sqlStringMap(record.diagnosticDetails)} AS diagnostic_details,
              ${sqlTimestamp(record.startTime)} AS start_time,
              ${sqlTimestamp(record.endTime)} AS end_time
            """.trimIndent()
        )
    }

    fun currentSparkAppId(): String? =
        sparkSessionProvider.getOrCreate().sparkContext().applicationId()

    fun currentSparkUser(): String? =
        sparkSessionProvider.getOrCreate().sparkContext().sparkUser()

    private fun sqlNullableString(value: String?): String =
        value?.let { sqlString(it) } ?: "CAST(NULL AS STRING)"

    private fun sqlString(value: String): String =
        "'${value.replace("'", "''")}'"

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

    companion object {
        const val AUDIT_TABLE_NAME =
            "spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs"
    }
}
