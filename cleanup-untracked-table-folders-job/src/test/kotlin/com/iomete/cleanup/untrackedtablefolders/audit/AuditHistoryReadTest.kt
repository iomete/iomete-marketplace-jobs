package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService.Companion.historyRow
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService.Companion.historyTrustProblem
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import scala.collection.JavaConverters

class AuditHistoryReadTest {
    private val columns =
        linkedMapOf(
            "run_id" to "STRING",
            "external_job_id" to "STRING",
            "catalog_name" to "STRING",
            "database_name" to "STRING",
            "status" to "STRING",
            "candidate_folders" to "ARRAY<STRING>",
            "deleted_folders" to "ARRAY<STRING>",
            "start_time" to "TIMESTAMP",
            "end_time" to "TIMESTAMP",
            "candidate_fingerprints" to "MAP<STRING, STRING>",
            "prev_run_id" to "STRING",
        )

    private fun schema(columns: Map<String, String> = this.columns): StructType =
        StructType.fromDDL(columns.entries.joinToString(", ") { (name, type) -> "$name $type" })

    private val iceberg: List<Row> =
        listOf(
            RowFactory.create("run_id", "string", null),
            RowFactory.create("", "", ""),
            RowFactory.create("# Detailed Table Information", "", ""),
            RowFactory.create("Name", "spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs", ""),
            RowFactory.create("Provider", "iceberg", ""),
        )

    private fun problem(
        describeRows: List<Row> = iceberg,
        schema: StructType = schema(),
        properties: Map<String, String?> = mapOf("format-version" to "2"),
        sessionWapSet: Boolean = false,
    ): String? = historyTrustProblem(describeRows, schema, properties, sessionWapSet)

    @Test
    fun `an Iceberg table with the expected columns and no write-audit-publish can be trusted`() {
        assertNull(problem())
        assertNull(problem(properties = mapOf("write.wap.enabled" to "false")))
    }

    @Test
    fun `a table that is not Iceberg is not trusted`() {
        assertEquals("the audit table is not an Iceberg table", problem(describeRows = iceberg.dropLast(1) + RowFactory.create("Provider", "parquet", "")))
        assertEquals("the audit table is not an Iceberg table", problem(describeRows = iceberg.dropLast(1)))
        assertEquals("the audit table is not an Iceberg table", problem(describeRows = iceberg + RowFactory.create("Provider", "iceberg", "")))
    }

    @Test
    fun `a missing, doubled or retyped history column is not trusted`() {
        assertEquals(
            "audit column candidate_fingerprints is missing or not of type map<string,string>",
            problem(schema = schema(columns - "candidate_fingerprints")),
        )
        assertEquals(
            "audit column end_time is missing or not of type timestamp",
            problem(schema = schema(columns + ("end_time" to "TIMESTAMP_NTZ"))),
        )
        assertEquals(
            "audit column deleted_folders is missing or not of type array<string>",
            problem(schema = schema(columns + ("deleted_folders" to "STRING"))),
        )
        assertEquals(
            "audit column prev_run_id is missing or not of type string",
            problem(schema = schema(columns).add("PREV_RUN_ID", "string")),
        )
    }

    @Test
    fun `write-audit-publish on the table or in the session is not trusted`() {
        assertEquals("write-audit-publish is enabled on the audit table", problem(properties = mapOf("write.wap.enabled" to " TRUE ")))
        assertEquals("the session sets a write-audit-publish id or branch", problem(sessionWapSet = true))
    }

    @Test
    fun `a row is read with its nulls kept, and a null inside a list or map makes the whole value missing`() {
        val end = Instant.parse("2026-11-20T10:00:00.123456Z")
        val schema = schema()
        fun row(vararg values: Any?): Row = GenericRowWithSchema(values, schema)
        fun seq(vararg values: String?) = JavaConverters.asScalaBuffer(values.toMutableList()).toSeq()
        fun map(vararg entries: Pair<String?, String?>) = JavaConverters.mapAsScalaMap(mutableMapOf(*entries))

        val full =
            historyRow(
                row("r1", "job", "cat", "db", "SUCCESS", seq("a"), seq(), Timestamp.from(end), Timestamp.from(end), map("a" to "fp1:x"), "r0"),
            )
        assertEquals(AuditHistoryRow("r1", "SUCCESS", end, listOf("a"), emptyList(), mapOf("a" to "fp1:x"), "r0"), full)

        val nulls = historyRow(row(null, "job", "cat", "db", null, null, null, null, null, null, null))
        assertEquals(AuditHistoryRow(null, null, null, null, null, null, null), nulls)

        val holes = historyRow(row("r1", "job", "cat", "db", "SUCCESS", seq("a", null), seq(null), null, end, map("a" to null), null))
        assertEquals(AuditHistoryRow("r1", "SUCCESS", end, null, null, null, null), holes)
    }
}
