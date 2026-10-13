package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant

sealed interface StorageFolderReconciliation {
    val folders: List<StorageFolder>

    data class DeletionEligible(
        override val folders: List<StorageFolder>,
    ) : StorageFolderReconciliation

    data class OwnershipUnverified(
        override val folders: List<StorageFolder>,
        val unresolvedTableCount: Int,
    ) : StorageFolderReconciliation
}

class TooManyCandidateFoldersException(
    val candidateCount: Int,
    val maxCandidateFolders: Int,
    val candidateFolderPaths: List<String>,
) : RuntimeException(
    "Detected $candidateCount candidate untracked table folder(s), which exceeds max_candidate_folders_per_database=$maxCandidateFolders"
)

@ApplicationScoped
class UntrackedFolderCandidateDetector {
    private val logger = Logger.getLogger(UntrackedFolderCandidateDetector::class.java)

    fun detectCandidates(
        storageFolders: List<StorageFolder>,
        activeTableLocations: List<String>,
        excludedPaths: List<String>,
        maxCandidateFolders: Int,
        unresolvedTableCount: Int,
    ): StorageFolderReconciliation {
        require(maxCandidateFolders >= 0) {
            "maxCandidateFolders must be greater than or equal to 0"
        }

        val candidateFolders =
            unmatchedFolders(
                storageFolders = storageFolders,
                activeTableLocations = activeTableLocations,
                excludedPaths = excludedPaths,
                stage = "candidate detection",
            )

        if (candidateFolders.size > maxCandidateFolders) {
            throw TooManyCandidateFoldersException(
                candidateCount = candidateFolders.size,
                maxCandidateFolders = maxCandidateFolders,
                candidateFolderPaths = candidateFolders.map { it.path },
            )
        }

        // Ownership certainty is a property of the whole database: one unresolved table makes
        // every unmatched folder unverifiable, never a mix.
        return if (unresolvedTableCount > 0) {
            StorageFolderReconciliation.OwnershipUnverified(candidateFolders, unresolvedTableCount)
        } else {
            StorageFolderReconciliation.DeletionEligible(candidateFolders)
        }
    }

    fun unmatchedFolders(
        storageFolders: List<StorageFolder>,
        activeTableLocations: List<String>,
        excludedPaths: List<String>,
        stage: String = "report-only root listing",
    ): List<StorageFolder> {
        val normalizedActiveTableLocations = activeTableLocations
            .map { StoragePathUtils.normalizeLocation(it) }

        val excludedPathSet = excludedPaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .toSet()

        val skipReasonCounts = mutableMapOf<CandidateSkipReason, Int>()

        val unmatchedFolders = storageFolders
            .filter { storageFolder ->
                val skipReason =
                    skipReason(
                        storageFolder = storageFolder,
                        normalizedActiveTableLocations = normalizedActiveTableLocations,
                        excludedPathSet = excludedPathSet,
                    )

                if (skipReason != null) {
                    skipReasonCounts.merge(skipReason, 1, Int::plus)
                    logSkippedFolder(
                        storageFolder = storageFolder,
                        skipReason = skipReason,
                    )
                }

                skipReason == null
            }
            .sortedBy { it.path }

        logSkipReasonSummary(
            stage = stage,
            totalFolderCount = storageFolders.size,
            skipReasonCounts = skipReasonCounts,
            reasons = CandidateSkipReason.entries - AGE_SKIP_REASONS,
        )

        return unmatchedFolders
    }

    fun applyAgeCutoff(
        reconciliation: StorageFolderReconciliation,
        folderStats: Map<String, StorageSizeStats>,
        cutoffTimeMillis: Long,
    ): StorageFolderReconciliation {
        val skipReasonCounts = mutableMapOf<CandidateSkipReason, Int>()

        val oldEnoughFolders =
            reconciliation.folders.filter { storageFolder ->
                val newestObjectModificationTimeMillis = folderStats[storageFolder.path]?.newestObjectModificationTimeMillis
                val skipReason = ageSkipReason(newestObjectModificationTimeMillis, cutoffTimeMillis)

                if (skipReason != null) {
                    skipReasonCounts.merge(skipReason, 1, Int::plus)
                    logSkippedFolder(
                        storageFolder = storageFolder,
                        skipReason = skipReason,
                        detail =
                            newestObjectModificationTimeMillis?.let {
                                "newestObjectModifiedAt=${Instant.ofEpochMilli(it)}, cutoffTime=${Instant.ofEpochMilli(cutoffTimeMillis)}"
                            },
                    )
                }

                skipReason == null
            }

        logSkipReasonSummary(
            stage = "the age check",
            totalFolderCount = reconciliation.folders.size,
            skipReasonCounts = skipReasonCounts,
            reasons = AGE_SKIP_REASONS,
        )

        return when (reconciliation) {
            is StorageFolderReconciliation.DeletionEligible ->
                StorageFolderReconciliation.DeletionEligible(oldEnoughFolders)

            is StorageFolderReconciliation.OwnershipUnverified ->
                reconciliation.copy(folders = oldEnoughFolders)
        }
    }

    private fun ageSkipReason(
        newestObjectModificationTimeMillis: Long?,
        cutoffTimeMillis: Long,
    ): CandidateSkipReason? =
        when {
            // S3A directory mtimes are synthetic, so only a readable object can prove a folder is old.
            newestObjectModificationTimeMillis == null || newestObjectModificationTimeMillis <= 0 ->
                CandidateSkipReason.UNKNOWN_AGE

            newestObjectModificationTimeMillis > cutoffTimeMillis -> CandidateSkipReason.TOO_NEW
            else -> null
        }

    private fun skipReason(
        storageFolder: StorageFolder,
        normalizedActiveTableLocations: List<String>,
        excludedPathSet: Set<String>,
    ): CandidateSkipReason? {
        val normalizedFolder = StoragePathUtils.normalizeLocation(storageFolder.path)
        val finalSegment = normalizedFolder.substringAfterLast('/')
        val matchedSentinelPrefix =
            SENTINEL_FOLDER_PREFIXES.firstOrNull { prefix ->
                finalSegment.startsWith(prefix)
            }

        if (matchedSentinelPrefix != null) {
            return CandidateSkipReason.SENTINEL_FOLDER
        }

        // Exact-match check is logically subsumed by the containment check below
        // (isSameOrChildLocation returns true for equal paths), but we keep both so
        // skip-reason logging distinguishes "candidate IS an active table" from
        // "candidate CONTAINS an active table." Do not collapse without preserving
        // both reasons explicitly.
        if (normalizedFolder in normalizedActiveTableLocations) {
            return CandidateSkipReason.ACTIVE_TABLE
        }

        val containsActiveTable =
            normalizedActiveTableLocations.any { activeLocation ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = activeLocation,
                    rootLocation = normalizedFolder,
                )
            }

        if (containsActiveTable) {
            return CandidateSkipReason.CONTAINS_ACTIVE_TABLE
        }

        val insideActiveTable =
            normalizedActiveTableLocations.any { activeLocation ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = normalizedFolder,
                    rootLocation = activeLocation,
                )
            }

        if (insideActiveTable) {
            return CandidateSkipReason.INSIDE_ACTIVE_TABLE
        }

        if (normalizedFolder in excludedPathSet) {
            return CandidateSkipReason.EXCLUDED_PATH
        }

        val containsExcludedPath =
            excludedPathSet.any { excludedPath ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = excludedPath,
                    rootLocation = normalizedFolder,
                )
            }

        if (containsExcludedPath) {
            return CandidateSkipReason.CONTAINS_EXCLUDED_PATH
        }

        val insideExcludedPath =
            excludedPathSet.any { excludedPath ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = normalizedFolder,
                    rootLocation = excludedPath,
                )
            }

        if (insideExcludedPath) {
            return CandidateSkipReason.INSIDE_EXCLUDED_PATH
        }

        return null
    }

    private fun logSkippedFolder(
        storageFolder: StorageFolder,
        skipReason: CandidateSkipReason,
        detail: String? = null,
    ) {
        if (!logger.isDebugEnabled) {
            return
        }

        val message =
            "Storage folder was not selected as cleanup candidate: path=${storageFolder.path}, reason=${skipReason.logValue}" +
                (detail?.let { ", $it" } ?: "")

        logger.debug(message)
    }

    private fun logSkipReasonSummary(
        stage: String,
        totalFolderCount: Int,
        skipReasonCounts: Map<CandidateSkipReason, Int>,
        reasons: Collection<CandidateSkipReason>,
    ) {
        if (skipReasonCounts.isEmpty()) {
            return
        }

        val totalSkipped = skipReasonCounts.values.sum()
        val perReasonSummary =
            reasons.joinToString(", ") { reason ->
                "${reason.logValue}=${skipReasonCounts[reason] ?: 0}"
            }

        logger.info(
            "Skipped $totalSkipped of $totalFolderCount storage folder(s) during $stage: $perReasonSummary. Enable DEBUG logging on ${UntrackedFolderCandidateDetector::class.java.name} for per-folder skip reasons."
        )
    }

    private enum class CandidateSkipReason(val logValue: String) {
        SENTINEL_FOLDER("sentinel_folder"),
        ACTIVE_TABLE("active_table"),
        CONTAINS_ACTIVE_TABLE("contains_active_table"),
        INSIDE_ACTIVE_TABLE("inside_active_table"),
        EXCLUDED_PATH("excluded_path"),
        CONTAINS_EXCLUDED_PATH("contains_excluded_path"),
        INSIDE_EXCLUDED_PATH("inside_excluded_path"),
        TOO_NEW("too_new"),
        UNKNOWN_AGE("unknown_age"),
    }

    private companion object {
        val AGE_SKIP_REASONS = listOf(CandidateSkipReason.TOO_NEW, CandidateSkipReason.UNKNOWN_AGE)

        val SENTINEL_FOLDER_PREFIXES = setOf(
            "_temporary",
            "_committed_",
            "_started_",
            ".spark-staging",
            ".hive-staging",
            "__magic",
        )
    }
}
