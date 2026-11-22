package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.candidate.HeldFolders
import com.iomete.cleanup.untrackedtablefolders.spark.PINNED_PARSER_SETTINGS
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSqlText
import com.iomete.cleanup.untrackedtablefolders.spark.VARIABLE_SUBSTITUTE
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.DirectoryCleanupOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStop
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStopCause
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionResult
import java.time.Instant
import org.antlr.v4.runtime.RuntimeMetaData
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val FOLDER = "s3a://example-bucket/db"

// The audit INSERT read by Spark 3.5's own SparkSqlParser with the session's pins: substitution off and
// escapedStringLiterals false. CatalogSqlTextTest checks that the session provider sets exactly these pins. It covers
// Spark's parser only, not a parser that a session extension puts in its place.
class AuditSqlSparkParserTest {
    private val service = CleanupAuditTableService()
    private val builder = CleanupAuditDiagnosticDetailsBuilder()

    @Test
    fun `the audit text is read by Spark's own parser on the test-only ANTLR runtime`() {
        // A method call, not the VERSION constant, which the compiler copies from the compile classpath.
        assertEquals("4.8", RuntimeMetaData.getRuntimeVersion())
        assertTrue(RuntimeMetaData::class.java.protectionDomain.codeSource.location.path.endsWith("/antlr4-runtime-4.8.jar"))
        val sql = service.buildInsertAuditRecordSql(record(mapOf("k" to "plain-value-7")))

        assertTrue(SparkSqlText(sql).plan is InsertIntoStatement)
        // A quote left unescaped ends the literal early, and the parser rejects the rest of the statement.
        assertThrows(ParseException::class.java) { SparkSqlText(sql.replace("'plain-value-7'", "'plain'-value-7'")).insert() }
    }

    @Test
    fun `every diagnostic value this change adds reads back exactly through Spark's parser, for every hostile value`() {
        HOSTILE_VALUES.forEach { hostile ->
            val details = changeDetails(hostile)
            assertEquals(emptyList<String>(), CHANGE_KEYS.filter { pattern -> details.keys.none { it.matches(pattern) } }, "keys missing for ${describe(hostile)}")
            val record = record(details).copy(status = "FAILED", statusReason = "deletion_stopped_at_recheck", errorMessage = hostile)

            val sql = service.buildInsertAuditRecordSql(record)
            val values = SparkSqlText(sql).insert().values
            // No asterisk is written raw, so a comment pattern run over the statement finds no unclosed comment.
            assertTrue("/*" !in sql, "value ${describe(hostile)}")
            // No dollar sign is written raw, so even a parser with substitution on reads the same values.
            val substituted = SparkSqlText(sql, PINNED_PARSER_SETTINGS + (VARIABLE_SUBSTITUTE to "true")).insert().values

            assertEquals(details, values["diagnostic_details"], "value ${describe(hostile)}")
            assertEquals("deletion_stopped_at_recheck", values["status_reason"])
            assertEquals(hostile, values["error_message"], "value ${describe(hostile)}")
            assertEquals(values, substituted, "value ${describe(hostile)} with substitution on")
        }
    }

    @Test
    fun `hostile text in diagnostic keys and folder lists reads back exactly through Spark's parser`() {
        val record =
            record(HOSTILE_VALUES.mapIndexed { index, value -> "$value#$index" to value }.toMap())
                .copy(candidateFolders = HOSTILE_VALUES, deletedFolders = HOSTILE_VALUES.reversed(), excludedPaths = HOSTILE_VALUES)

        val values = SparkSqlText(service.buildInsertAuditRecordSql(record)).insert().values

        assertEquals(record.diagnosticDetails, values["diagnostic_details"])
        assertEquals(HOSTILE_VALUES, values["candidate_folders"])
        assertEquals(HOSTILE_VALUES.reversed(), values["deleted_folders"])
        assertEquals(HOSTILE_VALUES, values["excluded_paths"])
    }

    // What the builder writes for the keys this change adds, from inputs that carry the hostile text in every path.
    private fun changeDetails(hostile: String): Map<String, String> {
        fun path(name: String) = "$FOLDER/${name}_$hostile"
        val partial = path("partial")
        val fence =
            DeletionFenceReport(
                keptPaths = FenceKeepReason.entries.associateWith { listOf(path(it.code)) },
                partialFolder =
                    FolderDeletionResult(
                        folderPath = partial,
                        outcome = FolderDeletionOutcome.PARTIAL_FAILED,
                        listedObjectCount = 4,
                        deletedKeys = listOf("deleted_$hostile"),
                        remainingKeys = listOf("remaining_$hostile"),
                        unknownKeys = listOf("unknown_$hostile"),
                        vanishedKeys = listOf("vanished_$hostile"),
                        leftoverObjectPaths = listOf("$partial/unlisted_$hostile"),
                        leftoverDirectoryPaths = listOf("$partial/kept_$hostile"),
                        unknownDirectoryPaths = listOf("$partial/unknown_$hostile"),
                        directoryCleanup = DirectoryCleanupOutcome.NOT_FINISHED,
                    ),
                notAttemptedPaths = listOf(path("next")),
                keptDirectoryPaths = listOf("${path("deleted")}/refused"),
                stop = FenceStop(FenceStopCause.SNAPSHOT_EXPIRED, "stopped"),
                ownershipRefreshCount = 2,
            )
        val inspection =
            mapOf(
                CandidateInspectionOutcome.TOO_MANY_OBJECTS to listOf(path("large")),
                CandidateInspectionOutcome.UNKNOWN_AGE to listOf(path("unknown_age")),
            )
        val tooNew = listOf(path("young"))
        return builder.deletionFenceDetails(fence) +
            builder.heldFolderDetails(HeldFolders.of(inspection, tooNew, fence)) +
            builder.inspectionOutcomeDetails(inspection) +
            builder.tooNewDetails(tooNew) +
            builder.ageDetails(configuredOlderThanHours = 24) +
            builder.wouldBeHeldByDeletionFloorDetails(listOf(path("floor")))
    }

    private fun record(details: Map<String, String>): CleanupAuditRecord =
        CleanupAuditRecord(
            runId = "run-1",
            sparkAppId = "spark-app-1",
            runtimeComputeId = null,
            runtimeComputeNamespace = null,
            runtimeDomain = null,
            runtimeUser = null,
            externalJobId = null,
            platformStartedBy = null,
            catalogName = "example_catalog",
            databaseName = "db",
            operation = "DELETE_UNTRACKED_TABLE_FOLDERS",
            dryRun = false,
            deleteEnabled = true,
            olderThanHours = 72,
            cutoffTime = Instant.parse("2026-05-18T10:00:00Z"),
            maxCandidateFoldersPerDatabase = 10,
            excludedPaths = emptyList(),
            status = "SUCCESS",
            statusReason = null,
            errorMessage = null,
            discoveredDatabaseLocation = FOLDER,
            storageScanLocation = FOLDER,
            activeTableCount = 1,
            unresolvedTableCount = 0,
            potentiallyUntrackedFolderCount = 1,
            storageFolderCount = 2,
            candidateFolderCount = 1,
            candidateObjectCount = 1,
            candidateTotalSizeBytes = 1,
            deletedFolderCount = 0,
            deletedObjectCount = 0,
            deletedTotalSizeBytes = 0,
            candidateFolders = emptyList(),
            deletedFolders = emptyList(),
            diagnosticDetails = details,
            startTime = Instant.parse("2026-05-21T10:00:00Z"),
            endTime = Instant.parse("2026-05-21T10:01:00Z"),
        )

    private fun describe(value: String): String = value.codePoints().toArray().joinToString(" ") { "U+%04X".format(it) }

    private companion object {
        // The diagnostic keys this change adds. folder_kept_at_final_check and deletion_failed_folder_path are older.
        val CHANGE_KEYS =
            listOf(
                "configured_older_than_hours",
                "deletion_min_age_hours",
                "deletion_stop_cause",
                "ownership_snapshot_max_age_seconds",
                "ownership_refresh_count",
                "held_folder_count",
                "held_folder_counts",
                "folder_not_attempted_(count|paths_sample|paths_truncated)",
                "folder_kept_(table_named_like_folder|changed_since_inspection|too_many_objects|too_new|unknown_age|key_not_addressable)_(count|paths_sample|paths_truncated)",
                "too_new_(count|paths_sample|paths_truncated)",
                "too_many_objects_(count|paths_sample|paths_truncated)",
                "would_be_held_by_deletion_floor_(count|paths_sample|paths_truncated)",
                "partially_deleted_folder_path",
                "partially_deleted_folder_listed_object_count",
                "partially_deleted_folder_(deleted|remaining|unknown_state|vanished|unlisted)_(object_count|keys_sample|keys_truncated)",
                "partially_deleted_folder_directory_cleanup",
                "partially_deleted_folder_(kept|unknown_state)_directory_(count|paths_sample|paths_truncated)",
                "deleted_folder_kept_directory_(count|paths_sample|paths_truncated)",
            ).map(::Regex)

        // Text that a lexer, a substitution step or a log viewer could read differently from the value it is.
        val HOSTILE_VALUES =
            listOf(
                "'",
                "''",
                "\\",
                "ends with a backslash\\",
                "\\'",
                "\\n",
                "\\\\",
                "\${spark.app.name}",
                "\${env:HOME}",
                "\${system:user.home}",
                "\${hiveconf:x}",
                "$",
                "$$",
                "a\$b\${",
                "caf\u00E9 \u00FCber \u00F1",
                "\u6570\u636E\u6E56",
                "\uD83D\uDE00",
                "\u202Eevil\u202C",
                "\u2066x\u2069",
                "\n",
                "\r\n",
                "\t",
                "\u0000",
                "\u001b[2J",
                "\u007f",
                "\u0085",
                "\u2028",
                "\u2029",
                "\uFEFF",
                "\u200B",
                "\u00AD",
                "\uDB40\uDC41",
                "\\u0041",
                "\\U0001F600",
                "\\101",
                "\\%",
                "\\_",
                "\\0",
                "\\Z",
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
