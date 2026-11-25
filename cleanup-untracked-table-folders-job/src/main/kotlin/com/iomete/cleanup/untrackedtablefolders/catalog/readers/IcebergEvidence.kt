package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import org.apache.iceberg.TableMetadata
import org.apache.iceberg.TableProperties

internal val ICEBERG_WRITE_PATH_PROPERTIES = listOf(
    TableProperties.WRITE_DATA_LOCATION,
    TableProperties.WRITE_METADATA_LOCATION,
    TableProperties.OBJECT_STORE_PATH,
    TableProperties.WRITE_FOLDER_STORAGE_LOCATION,
)

/** Records a live Iceberg table from its current metadata. Reads no files. */
internal fun ReadAccumulator.icebergTable(name: List<String>, metadata: TableMetadata) {
    val properties = metadata.properties()
    location(name, LocationRole.TABLE, metadata.location())
    ICEBERG_WRITE_PATH_PROPERTIES.forEach { location(name, LocationRole.WRITE_PATH, properties[it]) }
    if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
        externalPathUse += ExternalPathUse(name, emptyList(), "custom_location_provider")
    }
    // Iceberg reads any value other than true as false. Such a table may use files outside its own paths.
    if (properties[TableProperties.GC_ENABLED]?.equals("true", ignoreCase = true) == false) {
        externalPathUse += ExternalPathUse(name, emptyList(), "gc_disabled")
    }
    val referenced = metadata.previousFiles().map { it.file() } +
        metadata.snapshots().mapNotNull { it.manifestListLocation() } +
        metadata.statisticsFiles().map { it.path() } +
        metadata.partitionStatisticsFiles().map { it.path() }
    liveIceberg += LiveIcebergObject(
        name = name,
        uuid = metadata.uuid(),
        isView = false,
        location = metadata.location(),
        metadataFileLocation = metadata.metadataFileLocation(),
        referencedFiles = referenced.filter { it.isNotBlank() },
        nameMapped = properties.containsKey(TableProperties.DEFAULT_NAME_MAPPING),
    )
}
