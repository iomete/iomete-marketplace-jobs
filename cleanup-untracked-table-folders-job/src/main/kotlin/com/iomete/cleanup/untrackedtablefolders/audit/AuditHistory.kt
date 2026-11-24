package com.iomete.cleanup.untrackedtablefolders.audit

import java.time.Duration
import java.time.Instant

/** How long a folder must have been seen unchanged, by earlier runs of the same job, before it may be deleted. */
val CONFIRMATION_WINDOW: Duration = Duration.ofHours(72)

/** How far back the history is read. A chain that needs older rows to reach [CONFIRMATION_WINDOW] confirms nothing. */
val HISTORY_LOOKBACK: Duration = Duration.ofDays(30)

/**
 * One earlier audit row of the same job, catalog and database, with the columns confirmation reads. Paths and names
 * are as stored: made safe by the recorder, so a current path is compared in that form too.
 */
data class AuditHistoryRow(
    val runId: String?,
    val status: String?,
    val endTime: Instant?,
    val candidateFolders: List<String>?,
    val deletedFolders: List<String>?,
    /** Fingerprint of each folder the run inspected, before its age check. Null on rows that inspected nothing. */
    val candidateFingerprints: Map<String, String>?,
    val prevRunId: String?,
)

sealed interface AuditHistory {
    /** The newest earlier row's run id, written as this run's prev_run_id. */
    val newestRunId: String?
        get() = null

    /** No folder was inspected, so the history was not read. */
    data object NotRead : AuditHistory

    /** The run has no external job id, so no earlier run can be told apart as the same job. */
    data object NoJobIdentity : AuditHistory

    data class Unreadable(val reason: String) : AuditHistory

    data class Read(val rows: List<AuditHistoryRow>) : AuditHistory {
        // Newest first, and a row without an end time before all, so the walk stops at it.
        private val finalRows: List<AuditHistoryRow> =
            rows.filter { it.status != AUDIT_STATUS_INTENT }.sortedWith(compareByDescending(nullsLast()) { it.endTime })

        private val intentRows: List<AuditHistoryRow> = rows.filter { it.status == AUDIT_STATUS_INTENT }

        override val newestRunId: String?
            get() = finalRows.firstOrNull()?.runId

        /**
         * True when every final row from the newest back to a SUCCESS row that ended at least [window] before
         * [listingTime] saw [folder] with [fingerprint], each of those rows names the next older one as its
         * prev_run_id, and no row in that span deleted or meant to delete the folder. A row with a missing value ends
         * the span, so lost or damaged history confirms nothing.
         */
        fun confirms(
            folder: String,
            fingerprint: String,
            listingTime: Instant,
            window: Duration = CONFIRMATION_WINDOW,
        ): Boolean {
            val latestAllowedEnd = listingTime.minus(window)
            finalRows.forEachIndexed { index, row ->
                val endTime = row.endTime ?: return false
                val deleted = row.deletedFolders ?: return false
                val seen = row.candidateFingerprints ?: return false
                if (row.runId == null || folder in deleted || seen[folder] != fingerprint) {
                    return false
                }
                if (index > 0 && finalRows[index - 1].prevRunId != row.runId) {
                    return false
                }
                val intendedSince =
                    intentRows.any { intent ->
                        val intentEnd = intent.endTime
                        intentEnd == null || intent.candidateFolders == null || (!intentEnd.isBefore(endTime) && folder in intent.candidateFolders)
                    }
                if (intendedSince) {
                    return false
                }
                if (row.status == AUDIT_STATUS_SUCCESS && !endTime.isAfter(latestAllowedEnd)) {
                    return true
                }
            }
            return false
        }
    }
}

const val AUDIT_STATUS_SUCCESS = "SUCCESS"
const val AUDIT_STATUS_INTENT = "INTENT"

/** The history could not be trusted, so nothing is confirmed. [message] names a cause, never a value read. */
class AuditHistoryUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
