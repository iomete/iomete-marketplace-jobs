package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class FolderLayoutTest {
    private fun layout(vararg keys: String): FolderLayout = FolderLayout.of(keys.map { ListedObject(it, 1, 1, null) })

    private fun shape(vararg keys: String): DataOnlyShape = (layout(*keys) as FolderLayout.DataOnly).shape

    @Test
    fun `any metadata file makes a metadata folder, with every metadata file to read`() {
        assertEquals(
            FolderLayout.Metadata(listOf("metadata/v1.metadata.json", "metadata/v2.gz.metadata.json", "metadata/v3.metadata.json.gz")),
            layout("data/part-00000-a.parquet", "metadata/v3.metadata.json.gz", "metadata/v1.metadata.json", "metadata/v2.gz.metadata.json"),
        )
    }

    @Test
    fun `a version hint without a metadata file is a metadata folder with nothing to read`() {
        assertEquals(FolderLayout.Metadata(emptyList()), layout("metadata/version-hint.text", "data/part-00000-a.parquet"))
    }

    @Test
    fun `only commit scratch at the top of the folder is committer scratch`() {
        assertEquals(DataOnlyShape.COMMITTER_SCRATCH, shape("_temporary/0/_temporary/attempt/part-00000"))
        assertEquals(DataOnlyShape.COMMITTER_SCRATCH, shape("__magic_job-1/x.pending", "_SUCCESS"))
        assertEquals(DataOnlyShape.COMMITTER_SCRATCH, shape(".spark-staging-1/part-00000", ".part-00000.crc"))
        assertEquals(DataOnlyShape.COMMITTER_SCRATCH, shape(".hive-staging_hive_2026/-ext-10000/000000_0"))
    }

    @Test
    fun `scratch below the top, a partition or another format's state is not committer scratch`() {
        assertEquals(DataOnlyShape.OTHER, shape("sub/_temporary/0/x"))
        assertEquals(DataOnlyShape.OTHER, shape("_temporary/dt=2026-01-01/x"))
        assertEquals(DataOnlyShape.OTHER, shape("_temporary/_delta_log/0.json"))
        assertEquals(DataOnlyShape.PLAIN_OUTPUT, shape("sub/_SUCCESS"))
    }

    @Test
    fun `Spark part files, in partitions too, are plain output`() {
        assertEquals(DataOnlyShape.PLAIN_OUTPUT, shape("part-00000-1a2b.snappy.parquet", "_SUCCESS"))
        assertEquals(DataOnlyShape.PLAIN_OUTPUT, shape("dt=2026-01-01/part-00003-1a2b.c000.snappy.parquet", "_temporary/0/x"))
        assertEquals(DataOnlyShape.PLAIN_OUTPUT, shape("part-00000"))
    }

    @Test
    fun `other files and format state are neither scratch nor plain output`() {
        assertEquals(DataOnlyShape.OTHER, shape("part-0.parquet"))
        assertEquals(DataOnlyShape.OTHER, shape("00000-0-data.parquet"))
        assertEquals(DataOnlyShape.OTHER, shape("_delta_log/00000000000000000000.json", "part-00000-a.parquet"))
        assertEquals(DataOnlyShape.OTHER, shape("_spark_metadata/0", "part-00000-a.parquet"))
        assertEquals(DataOnlyShape.OTHER, shape(".hoodie/hoodie.properties"))
        assertEquals(DataOnlyShape.OTHER, shape("_metadata", "part-00000-a.parquet"))
        assertEquals(DataOnlyShape.OTHER, shape("_common_metadata"))
        assertEquals(DataOnlyShape.OTHER, shape())
    }
}
