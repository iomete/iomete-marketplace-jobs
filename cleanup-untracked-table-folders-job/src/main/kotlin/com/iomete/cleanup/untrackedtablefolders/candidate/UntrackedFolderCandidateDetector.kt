package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant

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
                val skipReason =
                    skipReason(
                        storageFolder = storageFolder,
                        normalizedActiveTableLocations = normalizedActiveTableLocations,
                        excludedPathSet = excludedPathSet,
                        cutoffTimeMillis = cutoffTimeMillis,
                    )

                if (skipReason != null) {
                    logSkippedFolder(
                        storageFolder = storageFolder,
                        skipReason = skipReason,
                        cutoffTimeMillis = cutoffTimeMillis,
                    )
                }

                skipReason == null
            }
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

    private fun skipReason(
        storageFolder: StorageFolder,
        normalizedActiveTableLocations: List<String>,
        excludedPathSet: Set<String>,
        cutoffTimeMillis: Long,
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

        if (normalizedFolder in excludedPathSet) {
            return CandidateSkipReason.EXCLUDED_PATH
        }

        if (storageFolder.modificationTimeMillis > cutoffTimeMillis) {
            return CandidateSkipReason.TOO_NEW
        }

        return null
    }

    private fun logSkippedFolder(
        storageFolder: StorageFolder,
        skipReason: CandidateSkipReason,
        cutoffTimeMillis: Long,
    ) {
        val message = when (skipReason) {
            CandidateSkipReason.TOO_NEW ->
                "Storage folder was not selected as cleanup candidate: path=${storageFolder.path}, reason=${skipReason.logValue}, modifiedAt=${Instant.ofEpochMilli(storageFolder.modificationTimeMillis)}, cutoffTime=${Instant.ofEpochMilli(cutoffTimeMillis)}"
            else ->
                "Storage folder was not selected as cleanup candidate: path=${storageFolder.path}, reason=${skipReason.logValue}"
        }

        logger.info(message)
    }

    private enum class CandidateSkipReason(val logValue: String) {
        SENTINEL_FOLDER("sentinel_folder"),
        ACTIVE_TABLE("active_table"),
        CONTAINS_ACTIVE_TABLE("contains_active_table"),
        EXCLUDED_PATH("excluded_path"),
        TOO_NEW("too_new"),
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
