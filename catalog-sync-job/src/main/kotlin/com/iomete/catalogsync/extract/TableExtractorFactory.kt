package com.iomete.catalogsync.extract


import com.iomete.catalogsync.PresidioClient
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.extract.datasets.DatasourceV1LikeTableExtractor
import com.iomete.catalogsync.extract.datasets.GenericTableExtractor
import com.iomete.catalogsync.extract.datasets.IcebergTableExtractor
import com.iomete.catalogsync.extract.datasets.ViewExtractor
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import org.eclipse.microprofile.rest.client.inject.RestClient
import jakarta.inject.Singleton

@Singleton
class TableExtractorFactory(
    @RestClient private val presidioClient: PresidioClient,
    sparkSessionProvider: SparkSessionProvider
) {

    private val spark = sparkSessionProvider.sparkSession

    private val columnTagExtractor = ColumnTagExtractor(spark = spark, presidioClient = presidioClient)

    fun extractorFor(
        provider: String,
        isView: Boolean = false,
        schema: String,
        table: String
    ): TableExtractor {

        if (isView) return ViewExtractor(columnTagExtractor = columnTagExtractor, schema = schema, table = table)

        return when (provider) {
            "iceberg" -> IcebergTableExtractor(
                spark = spark, columnTagExtractor = columnTagExtractor, schema = schema, table = table
            )

            "parquet", "orc", "hive" -> DatasourceV1LikeTableExtractor(
                spark = spark,
                columnTagExtractor = columnTagExtractor,
                schema = schema, tableName = table
            )

            else -> GenericTableExtractor(spark = spark, schema = schema, tableName = table)
        }
    }
}
