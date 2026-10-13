package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

enum class UnsafeScanRootReason(val code: String) {
    CATALOG_WAREHOUSE_UNKNOWN("catalog_warehouse_unknown"),
    DATABASE_LOCATION_IS_BUCKET_ROOT("database_location_is_bucket_root"),
    DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT("database_location_contains_warehouse_root"),
    SCAN_ROOT_OUTSIDE_DATABASE_BOUNDARY("scan_root_outside_database_boundary"),
    INVALID_STORAGE_LOCATION("invalid_storage_location"),
}

class UnsafeScanRootException(
    val reason: UnsafeScanRootReason,
    message: String,
) : IllegalStateException(message)

@ApplicationScoped
class StorageScanLocationResolver {

    private val logger = Logger.getLogger(StorageScanLocationResolver::class.java)

    fun resolve(
        databaseLocation: String,
        activeTableLocations: List<String>,
        sharedWarehouseRoots: List<String>,
    ): String {
        val databaseRoots = parsed(databaseLocation) { StoragePathUtils.allowedDatabaseRoots(databaseLocation) }

        validateDatabaseBoundary(
            databaseLocation = databaseLocation,
            databaseRoots = databaseRoots,
            sharedWarehouseRoots = sharedWarehouseRoots,
        )

        val inferredScanLocations =
            activeTableLocations
                .mapNotNull { location -> parsed(location) { parentLocation(location) } }
                .distinct()
                .sorted()

        val resolvedScanLocation =
            when (inferredScanLocations.size) {
                0 -> {
                    logger.warn(
                        "No active table locations were discovered for databaseLocation=$databaseLocation. Falling back to discovered database location for storage scan. This may miss untracked folders if the database location differs from the actual table storage root."
                    )
                    databaseLocation
                }
                1 -> inferredScanLocations.single()
                else -> {
                    logger.warn(
                        "Multiple active table parent locations were discovered for databaseLocation=$databaseLocation: $inferredScanLocations. Falling back to database location."
                    )
                    databaseLocation
                }
            }

        validateStorageScanLocation(
            databaseLocation = databaseLocation,
            databaseRoots = databaseRoots,
            storageScanLocation = resolvedScanLocation,
        )

        return resolvedScanLocation
    }

    private fun validateDatabaseBoundary(
        databaseLocation: String,
        databaseRoots: Set<String>,
        sharedWarehouseRoots: List<String>,
    ) {
        if (databaseRoots.any { StoragePathUtils.isStorageRoot(it) }) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.DATABASE_LOCATION_IS_BUCKET_ROOT,
                "Database location is a bucket root and cannot bound a cleanup scan. databaseLocation=$databaseLocation, allowedRoots=$databaseRoots",
            )
        }

        if (sharedWarehouseRoots.isEmpty()) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.CATALOG_WAREHOUSE_UNKNOWN,
                "Catalog warehouse location is not configured, so the database location cannot be checked against it. databaseLocation=$databaseLocation",
            )
        }

        val unqualifiedWarehouseRoot =
            sharedWarehouseRoots.firstOrNull { warehouseRoot ->
                !parsed(warehouseRoot) { StoragePathUtils.isQualifiedLocation(warehouseRoot) }
            }

        if (unqualifiedWarehouseRoot != null) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.INVALID_STORAGE_LOCATION,
                "Catalog warehouse location has no scheme or bucket, so the database location cannot be checked against it. databaseLocation=$databaseLocation, warehouseRoot=$unqualifiedWarehouseRoot",
            )
        }

        // A database boundary that covers the warehouse root can also cover other databases.
        val coveredWarehouseRoot =
            sharedWarehouseRoots.firstOrNull { warehouseRoot ->
                parsed(warehouseRoot) { StoragePathUtils.isInsideAnyRoot(warehouseRoot, databaseRoots) }
            }

        if (coveredWarehouseRoot != null) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT,
                "Database location equals or contains the catalog warehouse root, where other databases live. databaseLocation=$databaseLocation, warehouseRoot=$coveredWarehouseRoot",
            )
        }
    }

    private fun validateStorageScanLocation(
        databaseLocation: String,
        databaseRoots: Set<String>,
        storageScanLocation: String,
    ) {
        val insideDatabaseBoundary =
            parsed(storageScanLocation) { StoragePathUtils.isInsideAnyRoot(storageScanLocation, databaseRoots) }

        if (!insideDatabaseBoundary) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.SCAN_ROOT_OUTSIDE_DATABASE_BOUNDARY,
                "Resolved storage scan location escapes database boundary. databaseLocation=$databaseLocation, storageScanLocation=$storageScanLocation, allowedRoots=$databaseRoots",
            )
        }
    }

    private fun parentLocation(location: String): String? {
        if (StoragePathUtils.isStorageRoot(location)) {
            return null
        }

        val normalizedLocation = location.trim().trimEnd('/')
        val lastSlashIndex = normalizedLocation.lastIndexOf('/')

        return if (lastSlashIndex <= 0) {
            null
        } else {
            normalizedLocation.substring(0, lastSlashIndex)
        }
    }

    private fun <T> parsed(
        location: String,
        block: () -> T,
    ): T =
        try {
            block()
        } catch (error: IllegalArgumentException) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.INVALID_STORAGE_LOCATION,
                "Storage location cannot be parsed: $location (${error.message})",
            )
        }
}
