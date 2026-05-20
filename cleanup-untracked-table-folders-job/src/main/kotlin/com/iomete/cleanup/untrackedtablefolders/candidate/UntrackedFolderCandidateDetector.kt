package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped

class TooManyCandidateFoldersException(message: String) : RuntimeException(message)

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
                "Detected candidate folder count=${candidateFolders.size}, which exceeds max_candidate_folders_per_database=$maxCandidateFolders"
            )
        }

        return candidateFolders
    }
}
