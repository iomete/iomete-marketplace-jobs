package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped

enum class ScanRootSource {
    DATABASE_LOCATION,
    REST_NAMESPACE,
    TABLE_PARENT,
    APPROVED,
}

enum class ScanRootRejection(val code: String) {
    INVALID_LOCATION("invalid_location"),
    BUCKET_ROOT("bucket_root"),
    CONTAINS_WAREHOUSE_ROOT("contains_warehouse_root"),
    OUTSIDE_DATABASE_BOUNDARY("outside_database_boundary"),
    INSIDE_TABLE_LOCATION("inside_table_location"),
    CONTAINS_DATABASE_STORAGE("contains_database_storage"),
    NO_VISIBLE_TABLE("no_visible_table"),
    ROOT_LIMIT_REACHED("root_limit_reached"),
}

data class PlannedScanRoot(
    val location: String,
    val sources: Set<ScanRootSource>,
    val cleanupRoot: Boolean,
    val rejection: ScanRootRejection? = null,
    val approved: Boolean = false,
) {
    val reportOnly: Boolean get() = !cleanupRoot && rejection == null
}

enum class ScanRootOutcome(val code: String) {
    SCANNED("scanned"),
    ABSENT("absent"),
    LISTING_FAILED("listing_failed"),
    NOT_SCANNED("not_scanned"),
}

data class ScanRootResult(
    val root: PlannedScanRoot,
    val outcome: ScanRootOutcome,
    val visibleTableCount: Int,
    val unmatchedFolderPaths: List<String> = emptyList(),
) {
    val role: String
        get() =
            when {
                root.cleanupRoot -> "cleanup"
                root.reportOnly && root.approved -> "report_only_approved"
                root.reportOnly -> "report_only"
                else -> "not_scanned"
            }

    val status: String get() = root.rejection?.code ?: outcome.code
}

data class ScanPlanReport(
    val roots: List<ScanRootResult>,
    val uncoveredTableLocations: List<String>,
    // Set only when planning threw; roots and coverage are then empty and must not be read as a result.
    val planningError: String? = null,
    val restNamespaceRootStatus: String? = null,
) {
    val reportOnlyFolderPaths: List<String>
        get() = roots.flatMap { it.unmatchedFolderPaths }.distinct().sorted()

    // Coverage is measured against visible tables only, so "complete" never proves every table was seen.
    val complete: Boolean
        get() =
            planningError == null &&
                uncoveredTableLocations.isEmpty() &&
                roots.none {
                    it.outcome == ScanRootOutcome.LISTING_FAILED ||
                        it.root.rejection == ScanRootRejection.ROOT_LIMIT_REACHED
                }
}

const val MAX_REPORT_ONLY_SCAN_ROOTS = 25

@ApplicationScoped
class StorageScanLocationPlanner {

    // Only the cleanup root can delete. Other roots are listed for reporting only when they sit inside the
    // database's own storage (its location, the .db twin, the managed REST namespace root) or inside a root
    // approved for this database. A visible table alone never makes its parent listable.
    fun plan(
        cleanupRoot: String?,
        databaseLocation: String?,
        restNamespaceRoot: String?,
        approvedRoots: List<String>,
        activeTableLocations: List<String>,
        warehouseRoots: List<String>,
    ): List<PlannedScanRoot> {
        val normalizedCleanupRoot = cleanupRoot?.let(StoragePathUtils::normalizeLocation)
        val proposedSources = linkedMapOf<String, MutableSet<ScanRootSource>>()

        fun propose(
            location: String,
            source: ScanRootSource,
        ) {
            val key = normalizedOrNull(location) ?: location.trim()
            proposedSources.getOrPut(key) { mutableSetOf() } += source
        }

        databaseLocation?.let { propose(it, ScanRootSource.DATABASE_LOCATION) }
        restNamespaceRoot?.let { propose(it, ScanRootSource.REST_NAMESPACE) }
        approvedRoots.forEach { propose(it, ScanRootSource.APPROVED) }
        activeTableLocations.forEach { tableLocation ->
            val parent = runCatching { StoragePathUtils.parentLocation(tableLocation) }.getOrNull()
            propose(parent ?: tableLocation, ScanRootSource.TABLE_PARENT)
        }
        normalizedCleanupRoot?.let { proposedSources.getOrPut(it) { mutableSetOf() } }

        val tableLocations = activeTableLocations.mapNotNull(::normalizedOrNull)
        val databaseStorage =
            databaseLocation?.let(StoragePathUtils::allowedDatabaseRoots).orEmpty() +
                listOfNotNull(restNamespaceRoot?.let(::normalizedOrNull))

        val normalizedWarehouseRoots = warehouseRoots.mapNotNull(::normalizedOrNull)

        fun structuralRejection(location: String): ScanRootRejection? =
            rejection(location, tableLocations, databaseStorage, normalizedWarehouseRoots)

        fun holdsVisibleTable(location: String): Boolean =
            tableLocations.any { StoragePathUtils.isSameOrChildLocation(it, location) }

        // Approval is evidence that a location is relevant to this database, never proof that it owns it.
        val approvedBoundaries =
            proposedSources
                .filter { ScanRootSource.APPROVED in it.value }
                .keys
                .filter { structuralRejection(it) == null && holdsVisibleTable(it) }
                .toSet()

        var acceptedReportOnlyRoots = 0

        return proposedSources.entries
            .sortedBy { it.key }
            .map { (location, sources) ->
                if (location == normalizedCleanupRoot) {
                    return@map PlannedScanRoot(location, sources, cleanupRoot = true)
                }

                val structural = structuralRejection(location)
                // A root that exists only because of an approval needs a visible table, even inside the database's own storage.
                val approvalOnly = sources == setOf(ScanRootSource.APPROVED)
                val insideDatabaseStorage = structural == null && StoragePathUtils.isInsideAnyRoot(location, databaseStorage)

                val rejection =
                    structural
                        ?: when {
                            insideDatabaseStorage && !approvalOnly -> null
                            ScanRootSource.APPROVED in sources && !holdsVisibleTable(location) -> ScanRootRejection.NO_VISIBLE_TABLE
                            ScanRootSource.APPROVED in sources -> null
                            StoragePathUtils.isInsideAnyRoot(location, approvedBoundaries) -> null
                            else -> ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY
                        }
                        ?: if (acceptedReportOnlyRoots >= MAX_REPORT_ONLY_SCAN_ROOTS) {
                            ScanRootRejection.ROOT_LIMIT_REACHED
                        } else {
                            acceptedReportOnlyRoots += 1
                            null
                        }

                PlannedScanRoot(
                    location = location,
                    sources = sources,
                    cleanupRoot = false,
                    rejection = rejection,
                    approved = rejection == null && (approvalOnly || !insideDatabaseStorage),
                )
            }
    }

    private fun rejection(
        location: String,
        tableLocations: List<String>,
        databaseStorage: Set<String>,
        warehouseRoots: List<String>,
    ): ScanRootRejection? =
        try {
            when {
                !StoragePathUtils.isQualifiedLocation(location) -> ScanRootRejection.INVALID_LOCATION
                StoragePathUtils.isStorageRoot(location) -> ScanRootRejection.BUCKET_ROOT
                warehouseRoots.any { StoragePathUtils.isSameOrChildLocation(it, location) } ->
                    ScanRootRejection.CONTAINS_WAREHOUSE_ROOT
                // Listing a table's own folder would report its data and metadata directories as unmatched.
                tableLocations.any { StoragePathUtils.isSameOrChildLocation(location, it) } ->
                    ScanRootRejection.INSIDE_TABLE_LOCATION
                databaseStorage.any { it != location && StoragePathUtils.isSameOrChildLocation(it, location) } ->
                    ScanRootRejection.CONTAINS_DATABASE_STORAGE
                else -> null
            }
        } catch (error: IllegalArgumentException) {
            ScanRootRejection.INVALID_LOCATION
        }

    private fun normalizedOrNull(location: String): String? =
        try {
            StoragePathUtils.normalizeLocation(location)
        } catch (error: IllegalArgumentException) {
            null
        }
}
