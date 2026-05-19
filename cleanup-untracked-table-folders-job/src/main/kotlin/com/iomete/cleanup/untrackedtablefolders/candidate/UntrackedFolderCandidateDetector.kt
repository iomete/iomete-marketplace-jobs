package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
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
            .map { normalizePath(it) }
            .toSet()

        val excludedPathSet = excludedPaths
            .map { normalizePath(it) }
            .toSet()

        val candidateFolders = storageFolders
            .filter { normalizePath(it.path) !in activeTableLocationSet }
            .filter { normalizePath(it.path) !in excludedPathSet }
            .filter { it.modificationTimeMillis <= cutoffTimeMillis }
            .sortedBy { it.path }

        if (candidateFolders.size > maxCandidateFolders) {
            throw TooManyCandidateFoldersException(
                "Detected candidate folder count=${candidateFolders.size}, which exceeds max_candidate_folders_per_database=$maxCandidateFolders"
            )
        }

        return candidateFolders
    }

    private fun normalizePath(path: String): String =
        path.trim().trimEnd('/')
}
