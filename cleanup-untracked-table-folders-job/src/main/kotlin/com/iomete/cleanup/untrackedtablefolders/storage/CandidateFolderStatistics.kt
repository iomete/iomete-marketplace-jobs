package com.iomete.cleanup.untrackedtablefolders.storage

enum class CandidateInspectionOutcome(val code: String) {
    FOLDER_NOT_FOUND("folder_not_found"),
    STATISTICS_UNAVAILABLE("statistics_unavailable"),
    EMPTY_FOLDER("empty_folder"),
    UNKNOWN_AGE("unknown_age"),
}

data class CandidateFolderStatistics(
    val statsByFolder: Map<String, StorageSizeStats>,
    val absentFolderPaths: Set<String> = emptySet(),
) {
    // Null means the folder has objects of known age, so only the age cutoff decides.
    fun outcome(folderPath: String): CandidateInspectionOutcome? {
        val stats = statsByFolder[folderPath]
        val newestObjectModificationTimeMillis = stats?.newestObjectModificationTimeMillis

        return when {
            folderPath in absentFolderPaths -> CandidateInspectionOutcome.FOLDER_NOT_FOUND
            stats == null -> CandidateInspectionOutcome.STATISTICS_UNAVAILABLE
            stats.objectCount == 0L -> CandidateInspectionOutcome.EMPTY_FOLDER
            // S3A directory mtimes are synthetic, so only a readable object can prove a folder is old.
            newestObjectModificationTimeMillis == null || newestObjectModificationTimeMillis <= 0 ->
                CandidateInspectionOutcome.UNKNOWN_AGE

            else -> null
        }
    }

    fun folderPathsByOutcome(folderPaths: List<String>): Map<CandidateInspectionOutcome, List<String>> =
        folderPaths
            .mapNotNull { path -> outcome(path)?.let { it to path } }
            .groupBy({ it.first }, { it.second })
            .mapValues { (_, paths) -> paths.sorted() }

    companion object {
        val EMPTY = CandidateFolderStatistics(emptyMap())
    }
}
