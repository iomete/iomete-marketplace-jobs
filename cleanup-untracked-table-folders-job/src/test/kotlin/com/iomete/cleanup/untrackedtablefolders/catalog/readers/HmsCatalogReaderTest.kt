package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import java.net.URI
import java.nio.file.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.AccessControlException
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.TableIdentifier as IcebergIdentifier
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.inmemory.InMemoryFileIO
import org.apache.iceberg.types.Types
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import scala.Option
import scala.Tuple2
import scala.collection.JavaConverters
import scala.collection.Seq

class HmsCatalogReaderTest {
    @TempDir
    lateinit var dir: Path

    private val schema = StructType().add("id", DataTypes.LongType).add("day", DataTypes.StringType)

    @Test
    fun `a complete read returns database, table, view, partition and symlink evidence`() {
        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "orders", location("sales/orders"), partitionedBy = listOf("day")), false)
        catalog.createPartitions(
            "sales",
            "orders",
            seq(listOf(partition("day", "d1", location("sales/orders/day=d1")), partition("day", "d2", location("moved/d2")))),
            false,
        )
        catalog.createTable(table("sales", "raw_view", null, viewText = "SELECT * FROM parquet.`s3a://lake/raw/clicks`"), false)
        catalog.createTable(
            table("sales", "manifest_table", location("sales/manifest_table"), inputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"),
            false,
        )

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.COMPLETE, result.status, result.gaps.toString())
        assertTrue(result.locations.contains(OwnedLocation(listOf("sales"), LocationRole.NAMESPACE, location("sales"))))
        assertTrue(result.locations.contains(OwnedLocation(listOf("sales", "orders"), LocationRole.TABLE, location("sales/orders"))))
        // The partition inside the table folder is already covered by the table; only the moved one is listed.
        assertEquals(
            listOf(location("moved/d2")),
            result.locations.filter { it.role == LocationRole.PARTITION }.map { it.location },
        )
        assertEquals(
            setOf(
                ExternalPathUse(listOf("sales", "raw_view"), listOf("s3a://lake/raw/clicks")),
                ExternalPathUse(listOf("sales", "manifest_table"), emptyList(), "symlink_manifest_table"),
            ),
            result.externalPathUse.toSet(),
        )
    }

    @Test
    fun `a table getTablesByName leaves out is loaded on its own`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun getTablesByName(db: String, tables: Seq<String>): Seq<CatalogTable> =
                seq(super.getTablesByName(db, tables).asList().filter { it.identifier().table() != "hidden" })
        }
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "visible", location("sales/visible")), false)
        catalog.createTable(table("sales", "hidden", location("sales/hidden")), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.COMPLETE, result.status)
        assertTrue(result.locations.contains(OwnedLocation(listOf("sales", "hidden"), LocationRole.TABLE, location("sales/hidden"))))
    }

    @Test
    fun `one unreadable table fails its batch, the others are read one by one and it stays a named gap`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun getTablesByName(db: String, tables: Seq<String>): Seq<CatalogTable> =
                throw IllegalStateException("cannot parse data type of broken")

            override fun getTable(db: String, table: String): CatalogTable =
                if (table == "broken") throw IllegalStateException("cannot parse data type") else super.getTable(db, table)
        }
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "good", location("sales/good")), false)
        catalog.createTable(table("sales", "broken", location("sales/broken")), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(listOf("sales", "broken"), ReadGapKind.FAILED, "load_table_failed", "IllegalStateException")), result.gaps)
        assertTrue(result.locations.any { it.name == listOf("sales", "good") })
    }

    @Test
    fun `a table dropped between listing and loading is a gap, not a missing owner`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun getTablesByName(db: String, tables: Seq<String>): Seq<CatalogTable> = seq(emptyList())

            override fun getTable(db: String, table: String): CatalogTable = throw NoSuchTableException(db, table)
        }
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "gone", location("sales/gone")), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals("NoSuchTableException", result.gaps.single().causeClass)
    }

    @Test
    fun `more partitions than the limit is a gap, not a partial list`() {
        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "orders", location("sales/orders"), partitionedBy = listOf("day")), false)
        catalog.createPartitions(
            "sales",
            "orders",
            seq((1..3).map { partition("day", "d$it", location("moved/d$it")) }),
            false,
        )

        val result = HmsCatalogReader("spark_catalog", catalog, null, limits = ReaderLimits(maxPartitionsPerTable = 2)).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(listOf("sales", "orders"), ReadGapKind.LIMIT_EXCEEDED, "too_many_partitions")), result.gaps)
        assertTrue(result.locations.none { it.role == LocationRole.PARTITION })
    }

    @Test
    fun `a metastore that refuses or fails the database listing reads nothing`() {
        val denied = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun listDatabases(): Seq<String> = throw AccessControlException("denied")
        }
        val broken = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun listDatabases(): Seq<String> = throw RuntimeException("metastore down")
        }

        assertEquals(CatalogReadStatus.UNAUTHORIZED, HmsCatalogReader("spark_catalog", denied, null).read().status)
        assertEquals(CatalogReadStatus.FAILED, HmsCatalogReader("spark_catalog", broken, null).read().status)
    }

    @Test
    fun `only the metastore's not-found answer makes a database missing, any other failure is a gap`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun getDatabase(db: String): CatalogDatabase =
                when (db) {
                    "locked" -> throw AccessControlException("denied")
                    "flaky" -> throw RuntimeException("metastore down")
                    else -> super.getDatabase(db)
                }
        }
        catalog.createDatabase(database("open"), false)
        catalog.createTable(table("open", "t", location("open/t")), false)
        val reader = HmsCatalogReader("spark_catalog", catalog, null)

        assertEquals(null, reader.readDatabase("gone"))
        assertEquals(listOf(ReadGap(listOf("locked"), ReadGapKind.UNAUTHORIZED, "load_database_failed", "AccessControlException")), reader.readDatabase("locked")!!.gaps)
        assertEquals(listOf(ReadGap(listOf("flaky"), ReadGapKind.FAILED, "load_database_failed", "RuntimeException")), reader.readDatabase("flaky")!!.gaps)
        val open = reader.readDatabase("open")!!
        assertEquals(CatalogReadStatus.COMPLETE, open.status, open.gaps.toString())
        assertEquals(listOf(listOf("open")), open.namespaces)
        assertTrue(open.locations.any { it.name == listOf("open", "t") && it.role == LocationRole.TABLE })
    }

    @Test
    fun `a failing table or partition listing in one database is a gap for that database`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun listTables(db: String): Seq<String> =
                if (db == "locked") throw AccessControlException("denied") else super.listTables(db)
        }
        catalog.createDatabase(database("locked"), false)
        catalog.createDatabase(database("open"), false)
        catalog.createTable(table("open", "t", location("open/t")), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(listOf("locked"), ReadGapKind.UNAUTHORIZED, "list_tables_failed", "AccessControlException")), result.gaps)
        assertTrue(result.locations.any { it.name == listOf("open", "t") })
    }

    @Test
    fun `an Iceberg table registered in the metastore is read from its metadata file`() {
        val iceberg = org.apache.iceberg.inmemory.InMemoryCatalog().apply { initialize("hms_iceberg", mapOf("warehouse" to "s3a://lake/wh")) }
        iceberg.createNamespace(Namespace.of("sales"))
        val events = iceberg.buildTable(IcebergIdentifier.of("sales", "events"), Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .withLocation("s3a://lake/wh/sales/events")
            .create()
        events.updateProperties().set("write.data.path", "s3a://lake/data/events").commit()
        val metadataLocation = (events as HasTableOperations).operations().current().metadataFileLocation()

        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(
            table("sales", "events", "s3a://lake/wh/sales/events", properties = mapOf("table_type" to "ICEBERG", "metadata_location" to metadataLocation)),
            false,
        )

        val result = HmsCatalogReader("spark_catalog", catalog, InMemoryFileIO()).read()
        val noIo = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.COMPLETE, result.status, result.gaps.toString())
        val live = result.liveIceberg.single()
        assertEquals(events.uuid().toString(), live.uuid)
        assertEquals(metadataLocation, live.metadataFileLocation)
        assertTrue(live.referencedFiles.isNotEmpty(), "the previous metadata file is referenced")
        assertTrue(result.locations.contains(OwnedLocation(listOf("sales", "events"), LocationRole.WRITE_PATH, "s3a://lake/data/events")))
        assertEquals(CatalogReadStatus.PARTIAL, noIo.status)
        assertEquals("iceberg_metadata_not_readable", noIo.gaps.single().reason)
    }

    @Test
    fun `a table with no location is a gap, not a table without storage`() {
        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "nowhere", null), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(listOf("sales", "nowhere"), ReadGapKind.UNVERIFIED, "table_location_missing")), result.gaps)
        assertTrue(result.locations.none { it.name == listOf("sales", "nowhere") })
    }

    @Test
    fun `a table with an unqualified location is a gap, and the location is not recorded`() {
        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "bare", "/warehouse/sales/bare"), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(
            listOf(ReadGap(listOf("sales", "bare"), ReadGapKind.UNVERIFIED, "table_location_not_qualified", location = "/warehouse/sales/bare")),
            result.gaps,
        )
        assertTrue(result.locations.none { it.name == listOf("sales", "bare") })
    }

    @Test
    fun `a JDBC table without a location stores no files, but one with an unqualified location is a gap`() {
        val catalog = catalog()
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "pg_orders", null, provider = "org.apache.spark.sql.jdbc"), false)
        catalog.createTable(table("sales", "pg_bare", "/warehouse/sales/pg_bare", provider = "jdbc"), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(
            listOf(ReadGap(listOf("sales", "pg_bare"), ReadGapKind.UNVERIFIED, "table_location_not_qualified", location = "/warehouse/sales/pg_bare")),
            result.gaps,
        )
        assertTrue(result.locations.none { it.role == LocationRole.TABLE })
    }

    @Test
    fun `partitions with an unqualified or missing location are gaps, qualified ones are kept`() {
        val catalog = object : InMemoryCatalog(SparkConf(), Configuration()) {
            override fun listPartitions(
                db: String,
                table: String,
                partialSpec: Option<scala.collection.immutable.Map<String, String>>,
            ): Seq<CatalogTablePartition> = seq(
                listOf(
                    partition("day", "d1", location("moved/d1")),
                    partition("day", "d2", "/warehouse/sales/orders/day=d2"),
                    CatalogTablePartition(
                        scalaMap(mapOf("day" to "d3")),
                        CatalogStorageFormat.empty(),
                        scalaMap(emptyMap()),
                        0L,
                        -1L,
                        Option.empty(),
                    ),
                ),
            )
        }
        catalog.createDatabase(database("sales"), false)
        catalog.createTable(table("sales", "orders", location("sales/orders"), partitionedBy = listOf("day")), false)

        val result = HmsCatalogReader("spark_catalog", catalog, null).read()

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(
            listOf(
                ReadGap(listOf("sales", "orders"), ReadGapKind.UNVERIFIED, "partition_location_not_qualified", location = "/warehouse/sales/orders/day=d2"),
                ReadGap(listOf("sales", "orders"), ReadGapKind.UNVERIFIED, "partition_location_missing"),
            ),
            result.gaps,
        )
        assertEquals(listOf(location("moved/d1")), result.locations.filter { it.role == LocationRole.PARTITION }.map { it.location })
    }

    private fun catalog() = InMemoryCatalog(SparkConf(), Configuration())

    private fun location(relative: String): String = dir.resolve(relative).toUri().toString().trimEnd('/')

    private fun database(name: String) = CatalogDatabase(name, "", URI(location(name)), scalaMap(emptyMap()))

    private fun table(
        database: String,
        name: String,
        location: String?,
        partitionedBy: List<String> = emptyList(),
        viewText: String? = null,
        inputFormat: String? = null,
        properties: Map<String, String> = emptyMap(),
        provider: String = "hive",
    ): CatalogTable {
        val storage = CatalogStorageFormat(
            Option.apply(location?.let(::URI)),
            Option.apply(inputFormat),
            Option.empty(),
            Option.empty(),
            false,
            scalaMap(emptyMap()),
        )
        return CatalogTable(
            TableIdentifier(name, Option.apply(database)),
            if (viewText != null) CatalogTableType.VIEW() else CatalogTableType.EXTERNAL(),
            storage,
            schema,
            Option.apply(if (viewText != null) null else provider),
            seq(partitionedBy),
            CatalogTable.`$lessinit$greater$default$7`(),
            CatalogTable.`$lessinit$greater$default$8`(),
            CatalogTable.`$lessinit$greater$default$9`(),
            CatalogTable.`$lessinit$greater$default$10`(),
            CatalogTable.`$lessinit$greater$default$11`(),
            scalaMap(properties),
            CatalogTable.`$lessinit$greater$default$13`(),
            Option.apply(viewText),
            CatalogTable.`$lessinit$greater$default$15`(),
            CatalogTable.`$lessinit$greater$default$16`(),
            true,
            CatalogTable.`$lessinit$greater$default$18`(),
            CatalogTable.`$lessinit$greater$default$19`(),
            CatalogTable.`$lessinit$greater$default$20`(),
        )
    }

    private fun partition(column: String, value: String, location: String) = CatalogTablePartition(
        scalaMap(mapOf(column to value)),
        CatalogStorageFormat(Option.apply(URI(location)), Option.empty(), Option.empty(), Option.empty(), false, scalaMap(emptyMap())),
        scalaMap(emptyMap()),
        System.currentTimeMillis(),
        -1L,
        Option.empty(),
    )

    private fun <T> seq(items: List<T>): Seq<T> = JavaConverters.asScalaBuffer(items)

    private fun scalaMap(map: Map<String, String>): scala.collection.immutable.Map<String, String> {
        var result: scala.collection.immutable.Map<String, String> = scala.collection.immutable.`Map$`.`MODULE$`.empty()
        map.forEach { (key, value) -> result = result.`$plus`(Tuple2(key, value)) }
        return result
    }
}
