package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.spark.ESCAPED_STRING_LITERALS
import com.iomete.cleanup.untrackedtablefolders.spark.PINNED_PARSER_SETTINGS
import com.iomete.cleanup.untrackedtablefolders.spark.SESSION_TIME_ZONE
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSqlText
import com.iomete.cleanup.untrackedtablefolders.spark.TIMESTAMP_TYPE
import com.iomete.cleanup.untrackedtablefolders.spark.VARIABLE_SUBSTITUTE
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.TimeZone
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CleanupAuditTableServiceTest {
    private val service = CleanupAuditTableService()

    @Test
    fun `builds audit insert sql with nullable cutoff timestamp`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(cutoffTime = null)
            )

        assertTrue(sql.contains("INSERT INTO ${CleanupAuditTableService.AUDIT_TABLE_NAME} ("))
        assertTrue(sql.contains("cutoff_time,"))
        assertTrue(sql.contains("CAST(NULL AS TIMESTAMP) AS cutoff_time"))
        assertTrue(sql.contains("diagnostic_details"))
        assertFalse(sql.contains("metrics"))
    }

    @Test
    fun `builds audit insert sql with non-null cutoff timestamp`() {
        val cutoffTime = Instant.parse("2026-05-21T10:15:30Z")

        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(cutoffTime = cutoffTime)
            )

        assertTrue(sql.contains("TIMESTAMP '"))
        assertTrue(sql.contains("' AS cutoff_time"))
        assertFalse(sql.contains("CAST(NULL AS TIMESTAMP) AS cutoff_time"))
    }

    @Test
    fun `escapes string values in audit insert sql`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(
                    databaseName = "customer's_db",
                    errorMessage = "can't find schema",
                )
            )

        // Spark has no '' escape: two quotes end one literal and start the next, which drops the quote.
        assertTrue(sql.contains("'customer\\'s_db' AS database_name"))
        assertTrue(sql.contains("'can\\'t find schema' AS error_message"))
        val values = parsedValues(sql)
        assertEquals("customer's_db", values["database_name"])
        assertEquals("can't find schema", values["error_message"])
    }

    @Test
    fun `escapes backslashes in audit insert sql`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(errorMessage = "path\\with\\backslashes")
            )

        assertTrue(sql.contains("'path\\\\with\\\\backslashes' AS error_message"))
    }

    @Test
    fun `emits CAST NULL for size columns when size statistics are unavailable`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(
                    candidateObjectCount = null,
                    candidateTotalSizeBytes = null,
                    deletedObjectCount = null,
                    deletedTotalSizeBytes = null,
                )
            )

        assertTrue(sql.contains("CAST(NULL AS BIGINT) AS candidate_object_count"))
        assertTrue(sql.contains("CAST(NULL AS BIGINT) AS candidate_total_size_bytes"))
        assertTrue(sql.contains("CAST(NULL AS BIGINT) AS deleted_object_count"))
        assertTrue(sql.contains("CAST(NULL AS BIGINT) AS deleted_total_size_bytes"))
    }

    @Test
    fun `every audit value reads back exactly as Spark reads the text, for every literal kind`() {
        HOSTILE_VALUES.forEach { value ->
            val record = recordWith(value)
            val sql = service.buildInsertAuditRecordSql(record)

            assertEquals(expectedValues(record), parsedValues(sql), "value ${describe(value)}")
            assertFalse('$' in sql, "a raw dollar sign in the SQL text for ${describe(value)}")
            assertFalse('*' in sql, "a raw asterisk in the SQL text for ${describe(value)}")
            assertEquals(emptyList<String>(), hiddenCharacters(sql), "hidden characters in the SQL text for ${describe(value)}")
            assertEquals(plainSqlLineCount, sql.count { it == '\n' }, "a raw line break in the SQL text for ${describe(value)}")
        }
    }

    @Test
    fun `all hostile values together, long values and empty collections read back exactly`() {
        val all = HOSTILE_VALUES.joinToString("|")
        val long = (HOSTILE_VALUES.joinToString("") + "x").repeat(200)
        val record =
            recordWith(all).copy(
                errorMessage = long,
                excludedPaths = HOSTILE_VALUES,
                candidateFolders = emptyList(),
                deletedFolders = HOSTILE_VALUES.reversed(),
                diagnosticDetails = HOSTILE_VALUES.mapIndexed { index, value -> "$value#$index" to value }.toMap(),
            )
        val sql = service.buildInsertAuditRecordSql(record)

        assertEquals(expectedValues(record), parsedValues(sql))
        assertFalse('$' in sql)
        assertEquals(emptyList<String>(), hiddenCharacters(sql))

        val empty = record.copy(excludedPaths = emptyList(), diagnosticDetails = emptyMap())
        assertEquals(expectedValues(empty), parsedValues(service.buildInsertAuditRecordSql(empty)))
    }

    @Test
    fun `an unpaired surrogate is written escaped`() {
        // Not a round trip: Spark stores strings as UTF-8, which has no form for it, so it becomes '?' however written.
        val sql = service.buildInsertAuditRecordSql(recordWith("a\uD800b\uDC00"))

        assertTrue(sql.contains("'a\\uD800b\\uDC00' AS run_id"))
        assertEquals(emptyList<String>(), hiddenCharacters(sql))
    }

    @Test
    fun `nullable audit values are written as typed nulls`() {
        val record =
            recordWith("x").copy(
                sparkAppId = null,
                runtimeComputeId = null,
                runtimeComputeNamespace = null,
                runtimeDomain = null,
                runtimeUser = null,
                externalJobId = null,
                platformStartedBy = null,
                cutoffTime = null,
                statusReason = null,
                errorMessage = null,
                discoveredDatabaseLocation = null,
                candidateObjectCount = null,
                candidateTotalSizeBytes = null,
                deletedObjectCount = null,
                deletedTotalSizeBytes = null,
            )

        val values = parsedValues(service.buildInsertAuditRecordSql(record))

        assertEquals(expectedValues(record), values)
        assertEquals(15, values.count { it.value == null })
    }

    @Test
    fun `substitution would rewrite values if it were on, so the session pins it off and the text has no dollar sign`() {
        val home = System.getProperty("user.home")
        assertTrue(home.isNotEmpty())

        // Why the pin matters: with substitution on, a literal written as is takes the JVM's value.
        val plainLiteral = "SELECT '\${system:user.home}' AS v"
        assertEquals(home, projectedLiteral(plainLiteral, PINNED_PARSER_SETTINGS + (VARIABLE_SUBSTITUTE to "true")))
        assertEquals("\${system:user.home}", projectedLiteral(plainLiteral, PINNED_PARSER_SETTINGS))

        // The job's own text keeps the value even with substitution on, because no dollar sign is written raw.
        val record = recordWith("\${system:user.home}")
        val substituted = parsedValues(service.buildInsertAuditRecordSql(record), PINNED_PARSER_SETTINGS + (VARIABLE_SUBSTITUTE to "true"))
        assertEquals(expectedValues(record), substituted)

        // Why the other pin matters: a parser that keeps escapes as written would store them.
        val kept = parsedValues(service.buildInsertAuditRecordSql(recordWith("it's")), PINNED_PARSER_SETTINGS + (ESCAPED_STRING_LITERALS to "true"))
        assertNotEquals("it's", kept["run_id"])
    }

    @Test
    fun `audit timestamps keep their instant whatever the JVM and session time zones`() {
        val instants =
            listOf(
                // A local time that does not exist in Los Angeles, one that exists twice, and microsecond edges.
                Instant.parse("2026-03-08T10:30:00.123456789Z"),
                Instant.parse("2026-11-01T08:30:00Z"),
                Instant.parse("1969-12-31T23:59:59.999999Z"),
                Instant.EPOCH,
                Instant.parse("2026-05-21T23:59:59.000001Z"),
                // Before the Gregorian switch, where java.sql.Timestamp's calendar and Spark's disagree.
                Instant.parse("1500-03-01T00:00:00Z"),
            )
        val defaultZone = TimeZone.getDefault()
        try {
            val sqlByJvmZone =
                listOf("UTC", "Asia/Tokyo", "America/Los_Angeles", "Pacific/Kiritimati").associateWith { jvmZone ->
                    TimeZone.setDefault(TimeZone.getTimeZone(jvmZone))
                    instants.map { instant ->
                        service.buildInsertAuditRecordSql(
                            recordWith("t").copy(cutoffTime = instant.minus(Duration.ofHours(72)), startTime = instant, endTime = instant.plusNanos(1_000)),
                        )
                    }
                }

            assertEquals(1, sqlByJvmZone.values.distinct().size, "the SQL text depends on the JVM time zone")
            sqlByJvmZone.forEach { (jvmZone, sqls) ->
                instants.zip(sqls).forEach { (instant, sql) ->
                    listOf("UTC", "America/Los_Angeles", "Asia/Kolkata", "+14:00").forEach { sessionZone ->
                        val values = parsedValues(sql, PINNED_PARSER_SETTINGS + (SESSION_TIME_ZONE to sessionZone))
                        val context = "instant=$instant jvm=$jvmZone session=$sessionZone"
                        assertEquals(instant.truncatedTo(ChronoUnit.MICROS), values["start_time"], context)
                        assertEquals(instant.plusNanos(1_000).truncatedTo(ChronoUnit.MICROS), values["end_time"], context)
                        assertEquals(instant.minus(Duration.ofHours(72)).truncatedTo(ChronoUnit.MICROS), values["cutoff_time"], context)
                    }
                }
            }

            // Spark reads a TIMESTAMP literal that names its zone as an instant even where TIMESTAMP means TIMESTAMP_NTZ.
            instants.zip(sqlByJvmZone.getValue("UTC")).forEach { (instant, sql) ->
                val values = parsedValues(sql, PINNED_PARSER_SETTINGS + (TIMESTAMP_TYPE to "TIMESTAMP_NTZ"))
                assertEquals(instant.truncatedTo(ChronoUnit.MICROS), values["start_time"], "instant=$instant")
                assertEquals(instant.plusNanos(1_000).truncatedTo(ChronoUnit.MICROS), values["end_time"], "instant=$instant")
                assertEquals(instant.minus(Duration.ofHours(72)).truncatedTo(ChronoUnit.MICROS), values["cutoff_time"], "instant=$instant")
            }
        } finally {
            TimeZone.setDefault(defaultZone)
        }
    }

    private fun recordWith(value: String): CleanupAuditRecord =
        CleanupAuditRecord(
            runId = value,
            sparkAppId = "app#$value",
            runtimeComputeId = "compute#$value",
            runtimeComputeNamespace = "namespace#$value",
            runtimeDomain = "domain#$value",
            runtimeUser = "user#$value",
            externalJobId = "job#$value",
            platformStartedBy = "starter#$value",
            catalogName = "catalog#$value",
            databaseName = "database#$value",
            operation = "operation#$value",
            dryRun = false,
            deleteEnabled = true,
            olderThanHours = 72,
            cutoffTime = Instant.parse("2026-05-18T10:00:00.000001Z"),
            maxCandidateFoldersPerDatabase = 7,
            excludedPaths = listOf(value, "excluded#$value"),
            status = "status#$value",
            statusReason = "reason#$value",
            errorMessage = "error#$value",
            discoveredDatabaseLocation = "location#$value",
            storageScanLocation = "scan#$value",
            activeTableCount = 1,
            unresolvedTableCount = 2,
            potentiallyUntrackedFolderCount = 3,
            storageFolderCount = 4,
            candidateFolderCount = 5,
            candidateObjectCount = 6,
            candidateTotalSizeBytes = Long.MAX_VALUE,
            deletedFolderCount = 8,
            deletedObjectCount = 9,
            deletedTotalSizeBytes = 0,
            candidateFolders = listOf(value, "candidate#$value"),
            deletedFolders = listOf(value),
            diagnosticDetails = mapOf(value to value, "key#$value" to "value#$value"),
            startTime = Instant.parse("2026-05-21T10:00:00.123456Z"),
            endTime = Instant.parse("2026-05-21T10:01:00.654321Z"),
        )

    private fun expectedValues(record: CleanupAuditRecord): Map<String, Any?> =
        linkedMapOf(
            "run_id" to record.runId,
            "spark_app_id" to record.sparkAppId,
            "runtime_compute_id" to record.runtimeComputeId,
            "runtime_compute_namespace" to record.runtimeComputeNamespace,
            "runtime_domain" to record.runtimeDomain,
            "runtime_user" to record.runtimeUser,
            "external_job_id" to record.externalJobId,
            "platform_started_by" to record.platformStartedBy,
            "catalog_name" to record.catalogName,
            "database_name" to record.databaseName,
            "operation" to record.operation,
            "dry_run" to record.dryRun,
            "delete_enabled" to record.deleteEnabled,
            "older_than_hours" to record.olderThanHours,
            "cutoff_time" to record.cutoffTime?.truncatedTo(ChronoUnit.MICROS),
            "max_candidate_folders_per_database" to record.maxCandidateFoldersPerDatabase,
            "excluded_paths" to record.excludedPaths,
            "status" to record.status,
            "status_reason" to record.statusReason,
            "error_message" to record.errorMessage,
            "discovered_database_location" to record.discoveredDatabaseLocation,
            "storage_scan_location" to record.storageScanLocation,
            "active_table_count" to record.activeTableCount,
            "unresolved_table_count" to record.unresolvedTableCount,
            "potentially_untracked_folder_count" to record.potentiallyUntrackedFolderCount,
            "storage_folder_count" to record.storageFolderCount,
            "candidate_folder_count" to record.candidateFolderCount,
            "candidate_object_count" to record.candidateObjectCount,
            "candidate_total_size_bytes" to record.candidateTotalSizeBytes,
            "deleted_folder_count" to record.deletedFolderCount,
            "deleted_object_count" to record.deletedObjectCount,
            "deleted_total_size_bytes" to record.deletedTotalSizeBytes,
            "candidate_folders" to record.candidateFolders,
            "deleted_folders" to record.deletedFolders,
            "diagnostic_details" to record.diagnosticDetails,
            "start_time" to record.startTime.truncatedTo(ChronoUnit.MICROS),
            "end_time" to record.endTime.truncatedTo(ChronoUnit.MICROS),
        )

    // The values Spark would insert: one aliased expression per target column, in order.
    private fun parsedValues(
        sql: String,
        settings: Map<String, String> = PINNED_PARSER_SETTINGS,
    ): Map<String, Any?> {
        val insert = SparkSqlText(sql, settings).insert()
        assertEquals(listOf("spark_catalog", "iomete_system_db", "cleanup_untracked_table_folder_runs"), insert.table)
        assertEquals(insert.columns, insert.values.keys.toList(), "every target column has its own value, in order")
        return insert.values
    }

    private fun projectedLiteral(
        sql: String,
        settings: Map<String, String>,
    ): Any? = SparkSqlText(sql, settings).select().values.single()

    private fun hiddenCharacters(sql: String): List<String> =
        sql.codePoints().toArray().filter { codePoint ->
            codePoint != '\n'.code &&
                when (Character.getType(codePoint).toByte()) {
                    Character.CONTROL, Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR, Character.SURROGATE -> true
                    else -> false
                }
        }.map { "U+%04X".format(it) }

    private fun describe(value: String): String = value.codePoints().toArray().joinToString(" ") { "U+%04X".format(it) }

    private val plainSqlLineCount = service.buildInsertAuditRecordSql(recordWith("plain")).count { it == '\n' }

    private fun auditRecord(
        cutoffTime: Instant? = null,
        databaseName: String = "test_db",
        errorMessage: String? = null,
        candidateObjectCount: Long? = 5,
        candidateTotalSizeBytes: Long? = 12345,
        deletedObjectCount: Long? = 0,
        deletedTotalSizeBytes: Long? = 0,
    ): CleanupAuditRecord =
        CleanupAuditRecord(
            runId = "run-1",
            sparkAppId = "spark-app-1",
            runtimeComputeId = "compute-123",
            runtimeComputeNamespace = "spark-resources-1",
            runtimeDomain = "fde",
            runtimeUser = "hasan",
            externalJobId = "3e9342e7-89d3-42f8-a603-e00b783cca17",
            platformStartedBy = "fde_admin",
            catalogName = "spark_catalog",
            databaseName = databaseName,
            operation = "DISCOVER_UNTRACKED_TABLE_FOLDERS",
            dryRun = true,
            deleteEnabled = false,
            olderThanHours = 24,
            cutoffTime = cutoffTime,
            maxCandidateFoldersPerDatabase = 10,
            excludedPaths = listOf("s3a://bucket/db/protected"),
            status = "SUCCESS",
            statusReason = null,
            errorMessage = errorMessage,
            discoveredDatabaseLocation = "s3a://bucket/db.db",
            storageScanLocation = "s3a://bucket/db",
            activeTableCount = 1,
            unresolvedTableCount = 0,
            potentiallyUntrackedFolderCount = 0,
            storageFolderCount = 2,
            candidateFolderCount = 1,
            candidateObjectCount = candidateObjectCount,
            candidateTotalSizeBytes = candidateTotalSizeBytes,
            deletedFolderCount = 0,
            deletedObjectCount = deletedObjectCount,
            deletedTotalSizeBytes = deletedTotalSizeBytes,
            candidateFolders = listOf("s3a://bucket/db/orphan"),
            deletedFolders = emptyList(),
            diagnosticDetails = mapOf("candidate_folder_paths_truncated" to "false"),
            startTime = Instant.parse("2026-05-21T10:00:00Z"),
            endTime = Instant.parse("2026-05-21T10:01:00Z"),
        )

    private companion object {
        // Text that a lexer, a substitution step or a log viewer could read differently from the value it is.
        val HOSTILE_VALUES =
            listOf(
                "'",
                "''",
                "\\",
                "ends with a backslash\\",
                "\\'",
                "\\n",
                "\${spark.app.name}",
                "\${env:HOME}",
                "\${system:user.home}",
                "$",
                "$$",
                "a\$b\${",
                "caf\u00E9 \u00FCber \u00F1",
                "\u6570\u636E\u6E56 \u30C6\u30FC\u30D6\u30EB",
                "\uD83D\uDE00",
                "a\uD83D\uDE00b",
                "\u202Eevil\u202C",
                "\u05E9\u05DC\u05D5\u05DD \u200F \u0645\u0631\u062D\u0628\u0627 \u061C",
                "\u2066x\u2069",
                "\n",
                "\r",
                "\t",
                "\u0000",
                "\u001b",
                "\u007f",
                "\u0085",
                "\u2028",
                "\u2029",
                "\uFEFF",
                "\u200B",
                "\u00AD",
                // U+E0041, a tag character: invisible, and outside the 16-bit range.
                "\uDB40\uDC41",
                "\\u0041",
                "\\U0001F600",
                "\\101",
                "\\%",
                "\\_",
                "\\0",
                "\\Z",
                "\\\\",
                "--",
                "/* */",
                "/*",
                "x/*a/part-0/*b/*",
                "*/",
                "`",
                "'; DROP TABLE x; --",
                "",
            )
    }
}
