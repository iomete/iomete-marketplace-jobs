package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import org.apache.iceberg.BaseTable
import org.apache.iceberg.StaticTableOperations
import org.apache.iceberg.Table
import org.apache.iceberg.io.FileIO

/** A read-only table at one metadata file, so a table the catalog API can't load still gets a manifest read. */
object StaticIcebergTables {
    fun load(
        name: List<String>,
        metadataFileLocation: String,
        io: FileIO,
    ): Table {
        val operations = StaticTableOperations(metadataFileLocation, io)
        // Read now, so a missing or unreadable metadata file fails the load and not a later step.
        operations.current()
        return BaseTable(operations, name.joinToString("."))
    }
}
