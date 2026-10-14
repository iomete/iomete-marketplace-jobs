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
    LOOKS_LIKE_DATABASE_STORAGE("looks_like_database_storage"),
    OTHER_DATABASE_LOCATION("other_database_location"),
    OTHER_DATABASE_BOUNDARY_UNKNOWN("other_database_boundary_unknown"),
}

// Storage of the other databases configured for this run: normalized boundary root to database name.
// A database whose location could not be read is unresolved and is never treated as non-overlapping.
data class OtherDatabaseBoundaries(
    val roots: Map<String, String>,
    val unresolvedDatabases: List<String>,
)

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
    val unresolvedOtherDatabases: List<String> = emptyList(),
) {
    val reportOnlyFolderPaths: List<String>
        get() = roots.flatMap { it.unmatchedFolderPaths }.distinct().sorted()

    val unapprovedTableParents: List<String>
        get() =
            roots
                .filter {
                    ScanRootSource.TABLE_PARENT in it.root.sources &&
                        ScanRootSource.APPROVED !in it.root.sources &&
                        it.root.rejection == ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY
                }.map { it.root.location }

    val acceptedApprovedRoots: List<String>
        get() = roots.filter { it.root.reportOnly && it.root.approved && ScanRootSource.APPROVED in it.root.sources }.map { it.root.location }

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
    // otherDatabaseBoundaries is called at most once, and only for an approval or a suggestion outside that storage.
    fun plan(
        cleanupRoot: String?,
        databaseLocation: String?,
        restNamespaceRoot: String?,
        approvedRoots: List<String>,
        activeTableLocations: List<String>,
        warehouseRoots: List<String>,
        otherDatabaseBoundaries: () -> OtherDatabaseBoundaries = { OtherDatabaseBoundaries(emptyMap(), emptyList()) },
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

        fun insideOwnStorage(location: String): Boolean = StoragePathUtils.isInsideAnyRoot(location, databaseStorage)

        val otherDatabases by lazy(otherDatabaseBoundaries)

        // A bucket root or a location holding a warehouse says nothing about who owns a path under it.
        val otherDatabaseRoots by lazy {
            otherDatabases.roots.keys.filterNot { root ->
                StoragePathUtils.isStorageRoot(root) ||
                    normalizedWarehouseRoots.any { StoragePathUtils.isSameOrChildLocation(it, root) }
            }
        }

        fun otherDatabaseRejection(location: String): ScanRootRejection? =
            when {
                otherDatabaseRoots.any { StoragePathUtils.overlaps(location, it) } -> ScanRootRejection.OTHER_DATABASE_LOCATION
                otherDatabases.unresolvedDatabases.isNotEmpty() -> ScanRootRejection.OTHER_DATABASE_BOUNDARY_UNKNOWN
                else -> null
            }

        // Hive keeps a database in <warehouse>/<name>.db and the managed REST catalog in <warehouse>/<name>.
        fun looksLikeDatabaseStorage(location: String): Boolean =
            generateSequence(location) { StoragePathUtils.parentLocation(it) }
                .takeWhile { !StoragePathUtils.isStorageRoot(it) }
                .any { folder -> folder.endsWith(".db") || StoragePathUtils.parentLocation(folder) in normalizedWarehouseRoots }

        // Approval is evidence that a location is relevant to this database, never proof that it owns it.
        val approvedBoundaries =
            proposedSources
                .filter { ScanRootSource.APPROVED in it.value }
                .keys
                .filter {
                    structuralRejection(it) == null &&
                        holdsVisibleTable(it) &&
                        (insideOwnStorage(it) || otherDatabaseRejection(it) == null)
                }.toSet()

        var acceptedReportOnlyRoots = 0

        return proposedSources.entries
            .sortedBy { it.key }
            .map { (location, sources) ->
                if (location == normalizedCleanupRoot) {
                    return@map PlannedScanRoot(location, sources, cleanupRoot = true)
                }

                val structural = structuralRejection(location)
                val insideDatabaseStorage = structural == null && insideOwnStorage(location)

                val rejection =
                    structural
                        ?: when {
                            insideDatabaseStorage -> null
                            ScanRootSource.APPROVED in sources && !holdsVisibleTable(location) -> ScanRootRejection.NO_VISIBLE_TABLE
                            ScanRootSource.APPROVED in sources -> otherDatabaseRejection(location)
                            StoragePathUtils.isInsideAnyRoot(location, approvedBoundaries) -> null
                            looksLikeDatabaseStorage(location) -> ScanRootRejection.LOOKS_LIKE_DATABASE_STORAGE
                            else -> otherDatabaseRejection(location) ?: ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY
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
                    approved = rejection == null && !insideDatabaseStorage,
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
