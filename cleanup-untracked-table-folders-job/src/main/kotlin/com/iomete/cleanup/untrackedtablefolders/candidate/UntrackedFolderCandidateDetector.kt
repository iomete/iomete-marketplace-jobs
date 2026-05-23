package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

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
        cutoffTimeMillis: Long,
        maxCandidateFolders: Int,
    ): List<StorageFolder> {
        require(maxCandidateFolders >= 0) {
            "maxCandidateFolders must be greater than or equal to 0"
        }

        val normalizedActiveTableLocations = activeTableLocations
            .map { StoragePathUtils.normalizeLocation(it) }

        val excludedPathSet = excludedPaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .toSet()

        val candidateFolders = storageFolders
            .filter { storageFolder ->
                val normalizedFolder = StoragePathUtils.normalizeLocation(storageFolder.path)
                val finalSegment = normalizedFolder.substringAfterLast('/')
                val matchedSentinelPrefix = SENTINEL_FOLDER_PREFIXES.firstOrNull { prefix ->
                    finalSegment.startsWith(prefix)
                }
                if (matchedSentinelPrefix != null) {
                    logger.info(
                        "Skipping framework sentinel folder (never selected as cleanup candidate): path=${storageFolder.path}, matchedPrefix=$matchedSentinelPrefix"
                    )
                }
                matchedSentinelPrefix == null
            }
            .filter { storageFolder ->
                val normalizedFolder = StoragePathUtils.normalizeLocation(storageFolder.path)
                val claimedByActiveTable = normalizedActiveTableLocations.any { activeLocation ->
                    StoragePathUtils.isSameOrChildLocation(
                        candidateLocation = activeLocation,
                        rootLocation = normalizedFolder,
                    )
                }
                !claimedByActiveTable
            }
            .filter { StoragePathUtils.normalizeLocation(it.path) !in excludedPathSet }
            .filter { it.modificationTimeMillis <= cutoffTimeMillis }
            .sortedBy { it.path }

        if (candidateFolders.size > maxCandidateFolders) {
            throw TooManyCandidateFoldersException(
                candidateCount = candidateFolders.size,
                maxCandidateFolders = maxCandidateFolders,
                candidateFolderPaths = candidateFolders.map { it.path },
            )
        }

        return candidateFolders
    }

    private companion object {
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
