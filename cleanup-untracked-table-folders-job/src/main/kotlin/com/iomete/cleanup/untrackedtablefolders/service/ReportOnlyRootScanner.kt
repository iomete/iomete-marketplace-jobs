package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.PlannedScanRoot
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootRejection
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootResult
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class ReportOnlyRootScanner {
    private val logger = Logger.getLogger(ReportOnlyRootScanner::class.java)

    @Inject lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService
    @Inject lateinit var untrackedFolderCandidateDetector: UntrackedFolderCandidateDetector
    @Inject lateinit var excludePathResolver: ExcludePathResolver

    fun scan(
        catalog: String,
        database: String,
        roots: List<PlannedScanRoot>,
        activeTableLocations: List<String>,
    ): ScanPlanReport {
        val acceptedRootLocations = roots.filter { it.cleanupRoot || it.reportOnly }.map { it.location }

        val results =
            roots.map { root ->
                // The planner already found an invalid location unparseable, so it is never compared with table paths.
                val visibleTableCount =
                    if (root.rejection == ScanRootRejection.INVALID_LOCATION) {
                        0
                    } else {
                        activeTableLocations.count { StoragePathUtils.isSameOrChildLocation(it, root.location) }
                    }
                when {
                    root.cleanupRoot -> ScanRootResult(root, ScanRootOutcome.SCANNED, visibleTableCount)
                    !root.reportOnly -> ScanRootResult(root, ScanRootOutcome.NOT_SCANNED, visibleTableCount)
                    else -> scanReportOnlyRoot(catalog, database, root, visibleTableCount, acceptedRootLocations, activeTableLocations)
                }
            }

        val coveringRoots =
            results
                .filter { it.outcome == ScanRootOutcome.SCANNED || it.outcome == ScanRootOutcome.ABSENT }
                .map { it.root.location }
                .toSet()

        return ScanPlanReport(
            // Cleanup and listed roots first, so a long tail of unlisted parents never hides them in a bounded sample.
            roots =
                results.sortedWith(
                    compareBy({ !it.root.cleanupRoot }, { !it.root.reportOnly }, { -it.visibleTableCount }, { it.root.location }),
                ),
            uncoveredTableLocations =
                activeTableLocations.filterNot { StoragePathUtils.isInsideAnyRoot(it, coveringRoots) }.sorted(),
        )
    }

    private fun scanReportOnlyRoot(
        catalog: String,
        database: String,
        root: PlannedScanRoot,
        visibleTableCount: Int,
        acceptedRootLocations: List<String>,
        activeTableLocations: List<String>,
    ): ScanRootResult {
        val folders =
            try {
                objectStorageDiscoveryService.listImmediateChildFoldersIfPresent(catalog, root.location)
                    ?: return ScanRootResult(root, ScanRootOutcome.ABSENT, visibleTableCount)
            } catch (error: Exception) {
                logger.warn("Could not list report-only storage root for catalog=$catalog, database=$database, root=${root.location}", error)
                return ScanRootResult(root, ScanRootOutcome.LISTING_FAILED, visibleTableCount)
            }

        // A folder that is, or holds, another accepted root is reported by that root's own listing.
        val otherRoots = acceptedRootLocations - root.location
        val ownFolders =
            folders.filterNot { folder -> otherRoots.any { StoragePathUtils.isSameOrChildLocation(it, folder.path) } }

        val unmatchedFolders =
            untrackedFolderCandidateDetector.unmatchedFolders(
                storageFolders = ownFolders,
                activeTableLocations = activeTableLocations,
                excludedPaths = excludePathResolver.effectiveExcludedPaths(database, root.location),
            )

        return ScanRootResult(
            root = root,
            outcome = ScanRootOutcome.SCANNED,
            visibleTableCount = visibleTableCount,
            unmatchedFolderPaths = unmatchedFolders.map { it.path }.sorted(),
        )
    }
}
