package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped

class TooManyCandidateFoldersException(
    val candidateCount: Int,
    val maxCandidateFolders: Int,
    val candidateFolderPaths: List<String>,
) : RuntimeException(
    "Detected $candidateCount candidate untracked table folder(s), which exceeds max_candidate_folders_per_database=$maxCandidateFolders"
)

@ApplicationScoped
class UntrackedFolderCandidateDetector {
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

        val activeTableLocationSet = activeTableLocations
            .map { StoragePathUtils.normalizeLocation(it) }
            .toSet()

        val excludedPathSet = excludedPaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .toSet()

        val candidateFolders = storageFolders
            .filter { StoragePathUtils.normalizeLocation(it.path) !in activeTableLocationSet }
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
}
