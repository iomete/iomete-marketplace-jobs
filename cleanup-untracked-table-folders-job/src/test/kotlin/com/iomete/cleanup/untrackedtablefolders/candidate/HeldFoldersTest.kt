package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test

class HeldFoldersTest {

    @Test
    fun `reasons follow the age check and then the fence, and gone folders are not held`() {
        val held =
            HeldFolders.of(
                inspectionOutcomes =
                    mapOf(
                        CandidateInspectionOutcome.TOO_MANY_OBJECTS to listOf("s3a://b/db/crowded"),
                        CandidateInspectionOutcome.FOLDER_NOT_FOUND to listOf("s3a://b/db/gone"),
                        CandidateInspectionOutcome.UNKNOWN_AGE to listOf("s3a://b/db/no_time"),
                        CandidateInspectionOutcome.EMPTY_FOLDER to listOf("s3a://b/db/empty_a", "s3a://b/db/empty_b"),
                        CandidateInspectionOutcome.STATISTICS_UNAVAILABLE to listOf("s3a://b/db/unreadable"),
                    ),
                tooNewPaths = listOf("s3a://b/db/young"),
                fence =
                    DeletionFenceReport(
                        keptPaths =
                            mapOf(
                                FenceKeepReason.KEY_NOT_ADDRESSABLE to listOf("s3a://b/db/odd_key"),
                                FenceKeepReason.CLAIMED to listOf("s3a://b/db/claimed"),
                                FenceKeepReason.TOO_NEW to emptyList(),
                            ),
                        notAttemptedPaths = listOf("s3a://b/db/not_attempted"),
                    ),
            )

        assertEquals(
            listOf(
                "statistics_unavailable",
                "empty_folder",
                "too_new",
                "unknown_age",
                "too_many_objects",
                "folder_kept_at_final_check",
                "folder_kept_key_not_addressable",
            ),
            held.pathsByReason.keys.toList(),
        )
        assertEquals(8, held.count)
        assertEquals(
            "statistics_unavailable=1, empty_folder=2, too_new=1, unknown_age=1, too_many_objects=1, " +
                "folder_kept_at_final_check=1, folder_kept_key_not_addressable=1",
            held.countsText(", "),
        )
        assertFalse("s3a://b/db/gone" in held.paths)
        assertFalse("s3a://b/db/not_attempted" in held.paths)
    }

    @Test
    fun `nothing is held when no reason has a folder`() {
        val held = HeldFolders.of(emptyMap(), emptyList(), DeletionFenceReport())

        assertEquals(0, held.count)
        assertEquals(emptyMap<String, List<String>>(), held.pathsByReason)
        assertEquals("", held.countsText("\n"))
    }
}
