package com.iomete.catalogsync

import com.iomete.catalogsync.LakehouseMetadataExtractor.TableDescription
import com.iomete.catalogsync.extract.ColumnStat
import jakarta.inject.Inject
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import kotlin.test.Test
import kotlin.test.assertEquals

class ProcessTableColumnsTest {
 @Inject
 lateinit var lakehouseMetadataExtractor: LakehouseMetadataExtractor

 @Test
 fun testSum() {
//  val expected = 42
//  assertEquals(expected, 42)

  val mockRawColumns: List<Row> = listOf(
   RowFactory.create("field1", "string", null),
   RowFactory.create("field2", "string", null),
   RowFactory.create("field3", "string", null),
   RowFactory.create("# Partition Information", null, null),
   RowFactory.create("# col_name,data_type,comment", null, null),
   RowFactory.create("field1", "string", null),
   RowFactory.create(null, null, null),
   RowFactory.create("# Metadata Columns", null, null),
   RowFactory.create("_spec_id", "int", null),
   RowFactory.create("_partition", "struct<field1:string>", null),
   RowFactory.create("_file", "string", null),
   RowFactory.create("_pos", "bigint", null),
   RowFactory.create("_deleted", "boolean", null),
   RowFactory.create(null, null, null),
   RowFactory.create("# Detailed Table Information", null, null),
   RowFactory.create("Name", "iceberg_catalog.icedb.test_columns", null),
   RowFactory.create("Type", "MANAGED", null),
   RowFactory.create("Location", "tmp/ice_catalog/icedb/test_columns", null),
   RowFactory.create("Provider", "iceberg", null),
   RowFactory.create("Owner", "shashankchaudhary", null),
   RowFactory.create("Table Properties", "[current-snapshot-id=4937732155213656659,format=iceberg/parquet,format-version=2,write.parquet.compression-codec=zstd]", null)
  )

  val mockTableDescription = TableDescription(
   columns = listOf(
    ColumnMetadata(
     name = "id",
     dataType = "int",
     description = "Primary key",
     sortOrder = 1,
     isPartitionKey = false,
     stats = listOf(
      ColumnStat(name = "min", statValue = "1"),
      ColumnStat(name = "max", statValue = "100")
     ),
     tags = listOf("identifier", "critical")
    ),
    ColumnMetadata(
     name = "created_at",
     dataType = "timestamp",
     description = "Creation time",
     sortOrder = 2,
     isPartitionKey = true,
     stats = listOf(
      ColumnStat(name = "min", statValue = "2024-01-01T00:00:00Z"),
      ColumnStat(name = "max", statValue = "2025-01-01T00:00:00Z")
     ),
     tags = listOf("timestamp", "partition_key")
    )
   ),
   metadata = mapOf(
    "tableType" to "iceberg",
    "owner" to "data_eng",
    "createdBy" to "automated_pipeline"
   )
  )


  var tableDescription = lakehouseMetadataExtractor.processTableColumns(mockRawColumns);
  assertEquals(mockTableDescription, tableDescription);
 }
}