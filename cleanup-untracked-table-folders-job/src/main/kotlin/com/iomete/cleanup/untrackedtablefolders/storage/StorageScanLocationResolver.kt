package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

// IOMETE stores lakehouse data in S3-compatible storage, Google Cloud Storage, or Azure storage.
private val SUPPORTED_STORAGE_SCHEMES = setOf("s3", "s3a", "s3n", "gs", "abfs", "abfss", "wasb", "wasbs")

enum class UnsafeScanRootReason(val code: String) {
    CATALOG_WAREHOUSE_UNKNOWN("catalog_warehouse_unknown"),
    DATABASE_LOCATION_IS_BUCKET_ROOT("database_location_is_bucket_root"),
    DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT("database_location_contains_warehouse_root"),
    SCAN_ROOT_OUTSIDE_DATABASE_BOUNDARY("scan_root_outside_database_boundary"),
    INVALID_STORAGE_LOCATION("invalid_storage_location"),
}

const val MANAGED_ROOT_NOT_APPLICABLE = "not_applicable"
const val MANAGED_ROOT_USED = "used"
const val MANAGED_ROOT_REFUSED_PREFIX = "refused:"

fun managedRootRefused(reason: String): String = MANAGED_ROOT_REFUSED_PREFIX + reason

data class CleanupRoots(
    // The resolved root first when it is kept. Every location may delete.
    val locations: List<String>,
    val managedRootStatus: String,
)

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

        // s3:// and s3a:// spellings of one folder are one parent. The first spelling in sort order is kept.
        val inferredScanLocations =
            activeTableLocations
                .mapNotNull { location -> parsed(location) { StoragePathUtils.parentLocation(location) } }
                .sorted()
                .distinctBy { location -> parsed(location) { StoragePathUtils.normalizeLocation(location) } }

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

    // When tables leave the resolved root, the database folder without `.db` becomes a second cleanup root, but
    // only where this catalog itself puts the database's tables and a verified table is stored. A location
    // guessed for an unresolved table is never passed here. resolve() has already checked the twin against
    // bucket and warehouse roots.
    fun cleanupRoots(
        resolvedScanLocation: String,
        databaseLocation: String,
        activeTableLocations: List<String>,
        derivedRestNamespaceRoot: () -> String?,
    ): CleanupRoots {
        val resolvedRoot = StoragePathUtils.normalizeLocation(resolvedScanLocation)
        val primaryOnly = { status: String -> CleanupRoots(listOf(resolvedScanLocation), status) }

        val tablesOutside = activeTableLocations.filterNot { StoragePathUtils.isSameOrChildLocation(it, resolvedRoot) }
        val twin =
            StoragePathUtils.allowedDatabaseRoots(databaseLocation).singleOrNull { it != resolvedRoot }
        if (tablesOutside.isEmpty() || resolvedRoot != StoragePathUtils.normalizeLocation(databaseLocation) || twin == null) {
            return primaryOnly(MANAGED_ROOT_NOT_APPLICABLE)
        }

        val restRoot = derivedRestNamespaceRoot()?.let(StoragePathUtils::normalizeLocation)
        val refusal =
            when {
                restRoot == null -> "rest_root_not_derived"
                restRoot != twin -> "rest_root_differs"
                activeTableLocations.any { StoragePathUtils.isSameOrChildLocation(twin, it) } -> "table_at_or_above_root"
                tablesOutside.none { StoragePathUtils.isSameOrChildLocation(it, twin) } -> "no_visible_table"
                else -> null
            }
        if (refusal != null) {
            logger.info("Not using $twin as a second cleanup root for databaseLocation=$databaseLocation: $refusal")
            return primaryOnly(managedRootRefused(refusal))
        }

        val primaryHoldsTable = tablesOutside.size < activeTableLocations.size
        return CleanupRoots(
            locations = if (primaryHoldsTable) listOf(resolvedScanLocation, twin) else listOf(twin),
            managedRootStatus = MANAGED_ROOT_USED,
        )
    }

    private fun validateDatabaseBoundary(
        databaseLocation: String,
        databaseRoots: Set<String>,
        sharedWarehouseRoots: List<String>,
    ) {
        if (!parsed(databaseLocation) { StoragePathUtils.isQualifiedLocation(databaseLocation) }) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.INVALID_STORAGE_LOCATION,
                "Database location has no scheme or bucket, so it cannot be checked against the warehouse or table locations. databaseLocation=$databaseLocation",
            )
        }

        val scheme = StoragePathUtils.scheme(databaseLocation)
        if (scheme !in SUPPORTED_STORAGE_SCHEMES) {
            throw UnsafeScanRootException(
                UnsafeScanRootReason.INVALID_STORAGE_LOCATION,
                "Database location uses storage scheme '$scheme', which this job does not support. databaseLocation=$databaseLocation",
            )
        }

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
