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
              spark_app_id STRING,
              run_id STRING,
              initiated_by STRING,
              catalog_name STRING,
              database_name STRING,
              operation STRING,
              dry_run BOOLEAN,
              delete_enabled BOOLEAN,
              status STRING,
              discovered_database_location STRING,
              storage_scan_location STRING,
              active_table_count BIGINT,
              storage_folder_count BIGINT,
              candidate_folder_count BIGINT,
              deleted_folder_count BIGINT,
              candidate_folders ARRAY<STRING>,
              deleted_folders ARRAY<STRING>,
              excluded_paths ARRAY<STRING>,
              metrics MAP<STRING, STRING>,
              error_message STRING,
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
            INSERT INTO $AUDIT_TABLE_NAME
            SELECT
              ${sqlNullableString(record.sparkAppId)} AS spark_app_id,
              ${sqlString(record.runId)} AS run_id,
              ${sqlNullableString(record.initiatedBy)} AS initiated_by,
              ${sqlString(record.catalogName)} AS catalog_name,
              ${sqlString(record.databaseName)} AS database_name,
              ${sqlString(record.operation)} AS operation,
              ${record.dryRun} AS dry_run,
              ${record.deleteEnabled} AS delete_enabled,
              ${sqlString(record.status)} AS status,
              ${sqlNullableString(record.discoveredDatabaseLocation)} AS discovered_database_location,
              ${sqlString(record.storageScanLocation)} AS storage_scan_location,
              ${record.activeTableCount}L AS active_table_count,
              ${record.storageFolderCount}L AS storage_folder_count,
              ${record.candidateFolderCount}L AS candidate_folder_count,
              ${record.deletedFolderCount}L AS deleted_folder_count,
              ${sqlStringArray(record.candidateFolders)} AS candidate_folders,
              ${sqlStringArray(record.deletedFolders)} AS deleted_folders,
              ${sqlStringArray(record.excludedPaths)} AS excluded_paths,
              ${sqlStringMap(record.metrics)} AS metrics,
              ${sqlNullableString(record.errorMessage)} AS error_message,
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

    companion object {
        const val AUDIT_TABLE_NAME =
            "spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs"
    }
}
