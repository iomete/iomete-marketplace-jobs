package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.ExclusionRules
import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.BeforeEach

class MetadataScraperTest {
    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockCatalogServiceClient: CatalogClient
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockCoreServiceClient: CoreClient
    private lateinit var mockSparkMetadataReader: SparkMetadataReader
    private lateinit var mockTableMetadataExtractor: TableMetadataExtractor
    private lateinit var scraper: MetadataScraper

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockCatalogServiceClient = mockk()
        mockSparkSessionProvider = mockk()
        mockApplicationConfig = mockk()
        mockCoreServiceClient = mockk()
        mockSparkMetadataReader = mockk()
        mockTableMetadataExtractor = mockk()

        every { mockSparkSessionProvider.getSession(any()) } returns mockSparkSession
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules()

        scraper =
            MetadataScraper(
                mockSparkSessionProvider,
                mockApplicationConfig,
                mockSparkMetadataReader,
                mockTableMetadataExtractor,
                mockCoreServiceClient,
                mockCatalogServiceClient,
            )
    }
}
