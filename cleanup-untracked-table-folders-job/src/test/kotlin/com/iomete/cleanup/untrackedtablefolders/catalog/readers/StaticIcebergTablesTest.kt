package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.catalog.HmsTestCatalog
import java.nio.file.Path
import org.apache.iceberg.DataFiles
import org.apache.iceberg.FileFormat
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.inmemory.InMemoryFileIO
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

class StaticIcebergTablesTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `a name-mapped Iceberg table registered in the metastore is read from its metadata_location for the files it uses`() {
        val iceberg = InMemoryCatalog().apply { initialize("hms_iceberg", mapOf("warehouse" to "s3a://lake/wh")) }
        iceberg.createNamespace(Namespace.of("sales"))
        val imported =
            iceberg.buildTable(TableIdentifier.of("sales", "imported"), Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .withLocation("s3a://lake/wh/sales/imported")
                .withProperty("schema.name-mapping.default", """[{"field-id":1,"names":["id"]}]""")
                .create()
        imported.newAppend()
            .appendFile(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("s3a://lake/raw/part-00000.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1).build(),
            ).commit()
        val metadataLocation = (imported as HasTableOperations).operations().current().metadataFileLocation()
        val hms = HmsTestCatalog(dir)
        hms.database("sales", dir.resolve("sales").toUri().toString())
        hms.table(
            "sales",
            "imported",
            "s3a://lake/wh/sales/imported",
            properties = mapOf("table_type" to "ICEBERG", "metadata_location" to metadataLocation),
        )

        val live = HmsCatalogReader("spark_catalog", hms.catalog, InMemoryFileIO()).read().liveIceberg.single()
        val table = StaticIcebergTables.load(live.name, live.metadataFileLocation!!, InMemoryFileIO())
        val outcome = NameMappedManifestReader().referencedFiles(live.name, table, listOf("s3a://lake/raw", "s3a://lake/other"))

        assertTrue(live.nameMapped)
        assertEquals(metadataLocation, live.metadataFileLocation)
        assertEquals(listOf("s3a://lake/raw/part-00000.parquet"), (outcome as NameMappedManifestReader.Outcome.Read).filesInFolders)
    }

    @Test
    fun `a metadata file that can't be read fails the load`() {
        assertThrows<Exception> { StaticIcebergTables.load(listOf("sales", "gone"), "s3a://lake/wh/sales/gone/metadata/v9.metadata.json", InMemoryFileIO()) }
    }
}
