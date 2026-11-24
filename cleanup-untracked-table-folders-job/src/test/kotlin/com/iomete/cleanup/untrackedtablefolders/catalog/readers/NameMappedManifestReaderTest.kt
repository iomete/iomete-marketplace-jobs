package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import java.util.UUID
import org.apache.iceberg.DataFiles
import org.apache.iceberg.FileFormat
import org.apache.iceberg.FileMetadata
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.inmemory.InMemoryFileIO
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class NameMappedManifestReaderTest {
    private val catalog = InMemoryCatalog().apply { initialize("mem", mapOf("warehouse" to "s3a://lake/wh")) }
    private val name = listOf("sales", "imported")

    @Test
    fun `data and delete files of every snapshot and branch inside the folder are found`() {
        val table = table(nameMapped = true)
        table.newAppend().appendFile(dataFile("s3a://lake/raw/part-00000.parquet")).appendFile(dataFile("s3a://lake/other/part-1.parquet")).commit()
        table.newRowDelta().addDeletes(deleteFile("s3a://lake/raw/deletes/pos-1.parquet")).commit()
        table.manageSnapshots().createBranch("audit", table.currentSnapshot().snapshotId()).commit()
        table.newAppend().appendFile(dataFile("s3a://lake/raw/branch-only.parquet")).toBranch("audit").commit()
        table.newDelete().deleteFile("s3a://lake/raw/part-00000.parquet").commit()

        val outcome = NameMappedManifestReader().referencedFiles(name, table, listOf("s3a://lake/raw/"))

        // part-00000 was deleted from main, but older snapshots still name it.
        assertEquals(
            setOf("s3a://lake/raw/part-00000.parquet", "s3a://lake/raw/deletes/pos-1.parquet", "s3a://lake/raw/branch-only.parquet"),
            (outcome as NameMappedManifestReader.Outcome.Read).filesInFolders.toSet(),
        )
    }

    @Test
    fun `a file without scheme and bucket counts when its path is in the folder, and one that can't be placed always counts`() {
        val table = table(nameMapped = true)
        table.newAppend()
            .appendFile(dataFile("/raw/part-00000.parquet"))
            .appendFile(dataFile("/elsewhere/part-1.parquet"))
            .appendFile(dataFile("relative/part-2.parquet"))
            .commit()

        val outcome = NameMappedManifestReader().referencedFiles(name, table, listOf("s3a://lake/raw"))

        assertEquals(setOf("/raw/part-00000.parquet", "relative/part-2.parquet"), (outcome as NameMappedManifestReader.Outcome.Read).filesInFolders.toSet())
    }

    @Test
    fun `a table without a name mapping is not read`() {
        val table = table(nameMapped = false)
        table.newAppend().appendFile(dataFile("s3a://lake/raw/part-00000.parquet")).commit()

        assertEquals(NameMappedManifestReader.Outcome.NotNameMapped, NameMappedManifestReader().referencedFiles(name, table, listOf("s3a://lake/raw")))
    }

    @Test
    fun `too many manifests or entries is unknown, not a partial list`() {
        val table = table(nameMapped = true)
        table.newAppend().appendFile(dataFile("s3a://lake/raw/a.parquet")).commit()
        table.newAppend().appendFile(dataFile("s3a://lake/raw/b.parquet")).appendFile(dataFile("s3a://lake/raw/c.parquet")).commit()

        val manifests = NameMappedManifestReader(ReaderLimits(maxManifestsPerTable = 1)).referencedFiles(name, table, listOf("s3a://lake/raw"))
        val entries = NameMappedManifestReader(ReaderLimits(maxManifestEntriesPerTable = 2)).referencedFiles(name, table, listOf("s3a://lake/raw"))

        assertEquals(ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "too_many_manifests"), (manifests as NameMappedManifestReader.Outcome.Unknown).gap)
        assertEquals(ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "too_many_manifest_entries"), (entries as NameMappedManifestReader.Outcome.Unknown).gap)
    }

    @Test
    fun `an unreadable manifest is unknown`() {
        val table = table(nameMapped = true)
        table.newAppend().appendFile(dataFile("s3a://lake/raw/a.parquet")).commit()
        table.currentSnapshot().allManifests(table.io()).forEach { InMemoryFileIO().deleteFile(it.path()) }

        val outcome = NameMappedManifestReader().referencedFiles(name, table, listOf("s3a://lake/raw"))

        assertTrue(outcome is NameMappedManifestReader.Outcome.Unknown)
        assertEquals("manifest_read_failed", (outcome as NameMappedManifestReader.Outcome.Unknown).gap.reason)
    }

    private fun table(nameMapped: Boolean): Table {
        val namespace = Namespace.of("sales_" + UUID.randomUUID().toString().replace("-", ""))
        catalog.createNamespace(namespace)
        val builder = catalog.buildTable(TableIdentifier.of(namespace, "imported"), Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .withProperty("format-version", "2")
        if (nameMapped) builder.withProperty("schema.name-mapping.default", """[{"field-id":1,"names":["id"]}]""")
        return builder.create()
    }

    private fun dataFile(path: String) = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path).withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1).build()

    private fun deleteFile(path: String) = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes().withPath(path).withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1).build()
}
