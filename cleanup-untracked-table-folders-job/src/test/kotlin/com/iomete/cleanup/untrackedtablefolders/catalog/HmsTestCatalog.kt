package com.iomete.cleanup.untrackedtablefolders.catalog

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import scala.Option
import scala.Tuple2
import scala.collection.JavaConverters

/** A Spark in-memory metastore whose databases can have any location, including one without scheme and bucket. */
internal class HmsTestCatalog(private val localDir: java.nio.file.Path) {
    val catalog = InMemoryCatalog(SparkConf(), Configuration())

    // Created in a local folder, since the in-memory metastore creates it, then moved to [location].
    fun database(name: String, location: String) {
        catalog.createDatabase(CatalogDatabase(name, "", localDir.resolve(name).toUri(), scalaMap(emptyMap())), false)
        catalog.alterDatabase(CatalogDatabase(name, "", URI(location), scalaMap(emptyMap())))
    }

    fun table(
        database: String,
        name: String,
        location: String?,
        provider: String = "hive",
        inputFormat: String? = null,
        properties: Map<String, String> = emptyMap(),
    ) {
        val storage =
            CatalogStorageFormat(Option.apply(location?.let(::URI)), Option.apply(inputFormat), Option.empty(), Option.empty(), false, scalaMap(emptyMap()))
        val table =
            CatalogTable(
                TableIdentifier(name, Option.apply(database)),
                CatalogTableType.EXTERNAL(),
                storage,
                StructType().add("id", DataTypes.LongType),
                Option.apply(provider),
                JavaConverters.asScalaBuffer(emptyList<String>()),
                CatalogTable.`$lessinit$greater$default$7`(),
                CatalogTable.`$lessinit$greater$default$8`(),
                CatalogTable.`$lessinit$greater$default$9`(),
                CatalogTable.`$lessinit$greater$default$10`(),
                CatalogTable.`$lessinit$greater$default$11`(),
                scalaMap(properties),
                CatalogTable.`$lessinit$greater$default$13`(),
                Option.empty(),
                CatalogTable.`$lessinit$greater$default$15`(),
                CatalogTable.`$lessinit$greater$default$16`(),
                true,
                CatalogTable.`$lessinit$greater$default$18`(),
                CatalogTable.`$lessinit$greater$default$19`(),
                CatalogTable.`$lessinit$greater$default$20`(),
            )
        catalog.createTable(table, false)
    }

    private fun scalaMap(map: Map<String, String>): scala.collection.immutable.Map<String, String> {
        var result: scala.collection.immutable.Map<String, String> = scala.collection.immutable.`Map$`.`MODULE$`.empty()
        map.forEach { (key, value) -> result = result.`$plus`(Tuple2(key, value)) }
        return result
    }
}
