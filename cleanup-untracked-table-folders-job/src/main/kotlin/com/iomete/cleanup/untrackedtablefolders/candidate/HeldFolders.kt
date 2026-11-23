package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason

/**
 * Folders with no verified catalog owner that the run kept, by reason: the age check's reasons in its order, then the
 * storage checks', then the deletion fence's. A reason is named like the audit keys that list its folders: an
 * inspection outcome, [TOO_NEW], a storage hold, or folder_kept_<code>. A folder that was gone is not held, so folder_not_found and folder_not_found_at_deletion are
 * not reasons here.
 */
class HeldFolders private constructor(
    val pathsByReason: Map<String, List<String>>,
) {
    val count: Int = pathsByReason.values.sumOf { it.size }

    val paths: Set<String> = pathsByReason.values.flatten().toSet()

    fun countsText(separator: String): String = pathsByReason.entries.joinToString(separator) { (reason, paths) -> "$reason=${paths.size}" }

    companion object {
        const val TOO_NEW = "too_new"

        fun of(
            inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>>,
            tooNewPaths: List<String>,
            fence: DeletionFenceReport,
            storageHolds: StorageHolds = StorageHolds.NONE,
        ): HeldFolders {
            fun byOutcome(outcome: CandidateInspectionOutcome) = outcome.code to inspectionOutcomes[outcome].orEmpty()

            val reasons =
                listOf(
                    byOutcome(CandidateInspectionOutcome.STATISTICS_UNAVAILABLE),
                    byOutcome(CandidateInspectionOutcome.EMPTY_FOLDER),
                    TOO_NEW to tooNewPaths,
                    byOutcome(CandidateInspectionOutcome.UNKNOWN_AGE),
                    byOutcome(CandidateInspectionOutcome.TOO_MANY_OBJECTS),
                ) + storageHolds.pathsByReason.map { (reason, paths) -> reason.code to paths } + FenceKeepReason.entries.map { "folder_kept_${it.code}" to fence.keptPaths[it].orEmpty() }
            return HeldFolders(reasons.filter { it.second.isNotEmpty() }.toMap())
        }
    }
}
