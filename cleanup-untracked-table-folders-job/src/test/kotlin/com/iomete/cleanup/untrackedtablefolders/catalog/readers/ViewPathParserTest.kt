package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class ViewPathParserTest {
    private val parser = ViewPathParser()
    private val name = listOf("sales", "v")

    @Test
    fun `path reads are found in joins, unions, CTEs and subqueries`() {
        val sql = """
            WITH staged AS (SELECT * FROM delta.`s3a://lake/staged`)
            SELECT a.id FROM parquet.`s3a://lake/raw/a` a
            JOIN staged s ON a.id = s.id
            WHERE a.id IN (SELECT id FROM orc.`/mnt/lake/ids`)
            UNION ALL
            SELECT id FROM json.`s3://other-bucket/events` WHERE EXISTS (SELECT 1 FROM csv.`s3a://lake/flags`)
        """.trimIndent()

        assertEquals(
            setOf("s3a://lake/staged", "s3a://lake/raw/a", "/mnt/lake/ids", "s3://other-bucket/events", "s3a://lake/flags"),
            parser.pathUse(name, sql)!!.paths.toSet(),
        )
    }

    @Test
    fun `a view that reads only tables reads no path`() {
        assertNull(parser.pathUse(name, "SELECT o.id FROM spark_catalog.sales.orders o JOIN sales.customers c ON o.cid = c.id"))
    }

    @Test
    fun `missing or unparseable text is an unknown path use, never no path use`() {
        assertEquals(ExternalPathUse(name, emptyList(), "view_text_missing"), parser.pathUse(name, null))
        assertEquals(ExternalPathUse(name, emptyList(), "view_text_unparseable"), parser.pathUse(name, "SELECT FROM WHERE ("))
    }
}
