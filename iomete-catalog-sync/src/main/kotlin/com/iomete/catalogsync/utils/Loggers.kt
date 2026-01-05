package com.iomete.catalogsync.utils

import com.iomete.catalogsync.MetadataScraper
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

fun MetadataScraper.CatalogMetadata.log() {
    logger.info(
        "Processing catalog: {} finished! Total Schemas: {}, Total Tables: {}, Total Size: {} bytes, Total Files: {}",
        catalog,
        totalSchemaCount,
        totalTableCount,
        totalSizeInBytes,
        totalFiles,
    )
}

fun MetadataScraper.SchemaMetadata.log() {
    logger.info(
        "Processing schema: {} finished! Total Tables: {}, Views: {}, Total Size: {} bytes, Total Files: {}, Failed Tables: {}",
        schema,
        totalTableCount,
        totalViewCount,
        totalSizeInBytes,
        totalFiles,
        failedTableCount,
    )
}

fun MetadataScraper.TableMetadata.log() {
    logger.info(
        "Processing finished for table: {}.{}.{}",
        catalog,
        schema,
        name,
    )
}
