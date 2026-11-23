package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistory
import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistoryUnavailableException
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.audit.HISTORY_LOOKBACK
import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateFolderStatistics
import com.iomete.cleanup.untrackedtablefolders.storage.DataOnlyShape
import com.iomete.cleanup.untrackedtablefolders.storage.FolderLayout
import com.iomete.cleanup.untrackedtablefolders.storage.MetadataVerdict
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

/** Why a folder that passed every ownership and age check is still kept. The code names the reason in audit rows. */
enum class StorageHoldReason(val code: String) {
    INSPECTION_INCOMPLETE("inspection_incomplete"),
    METADATA_VIEW(MetadataVerdict.VIEW.code),
    METADATA_UNREADABLE(MetadataVerdict.UNREADABLE.code),
    METADATA_NO_UUID(MetadataVerdict.NO_UUID.code),
    METADATA_LOCATION_MISMATCH(MetadataVerdict.LOCATION_MISMATCH.code),
    DATA_ONLY_UNKNOWN_PROVENANCE("data_only_unknown_provenance"),
    DATA_ONLY_NOT_PLAIN_OUTPUT("data_only_not_plain_output"),
    CONFIRMATION_NO_JOB_ID("confirmation_no_job_id"),
    CONFIRMATION_HISTORY_UNREADABLE("confirmation_history_unreadable"),
    CONFIRMATION_PATH_NOT_EXACT("confirmation_path_not_exact"),
    AWAITING_CONFIRMATION("awaiting_confirmation"),
    ;

    companion object {
        fun of(verdict: MetadataVerdict): StorageHoldReason? =
            when (verdict) {
                MetadataVerdict.OWN_TABLE -> null
                MetadataVerdict.VIEW -> METADATA_VIEW
                MetadataVerdict.UNREADABLE -> METADATA_UNREADABLE
                MetadataVerdict.NO_UUID -> METADATA_NO_UUID
                MetadataVerdict.LOCATION_MISMATCH -> METADATA_LOCATION_MISMATCH
            }
    }
}

/** The folders held by the storage checks, by reason, and the history the run read for confirmation. */
data class StorageHolds(
    val pathsByReason: Map<StorageHoldReason, List<String>>,
    val history: AuditHistory,
) {
    val heldPaths: Set<String> = pathsByReason.values.flatten().toSet()

    companion object {
        val NONE = StorageHolds(emptyMap(), AuditHistory.NotRead)
    }
}

/**
 * Holds every folder whose storage does not show it is safe to delete: table metadata that is not this folder's own
 * table, data that is not only commit scratch, or a folder not yet seen unchanged by earlier runs of the same job for
 * the confirmation window. A folder gets the first reason that applies, in that order.
 */
@ApplicationScoped
class StorageHoldChecker {
    private val logger = SafeLogger.getLogger(StorageHoldChecker::class.java)

    @Inject lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService

    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService

    fun check(
        catalog: String,
        database: String,
        eligibleFolders: List<String>,
        statistics: CandidateFolderStatistics,
        history: AuditHistory,
        listingTime: Instant,
    ): StorageHolds {
        val held = linkedMapOf<StorageHoldReason, MutableList<String>>()
        val remaining = mutableListOf<String>()

        fun hold(
            reason: StorageHoldReason,
            path: String,
        ) {
            held.getOrPut(reason) { mutableListOf() } += path
        }

        val metadataKeys = mutableMapOf<String, List<String>>()
        eligibleFolders.forEach { path ->
            when (val layout = statistics.layoutsByFolder[path]) {
                null -> hold(StorageHoldReason.INSPECTION_INCOMPLETE, path)
                is FolderLayout.Metadata -> metadataKeys[path] = layout.metadataKeys
                is FolderLayout.DataOnly ->
                    when (layout.shape) {
                        DataOnlyShape.COMMITTER_SCRATCH -> remaining += path
                        DataOnlyShape.PLAIN_OUTPUT -> hold(StorageHoldReason.DATA_ONLY_UNKNOWN_PROVENANCE, path)
                        DataOnlyShape.OTHER -> hold(StorageHoldReason.DATA_ONLY_NOT_PLAIN_OUTPUT, path)
                    }
            }
        }
        val verdicts = objectStorageDiscoveryService.inspectMetadata(catalog, metadataKeys)
        metadataKeys.keys.forEach { path ->
            val reason = StorageHoldReason.of(verdicts[path] ?: MetadataVerdict.UNREADABLE)
            if (reason == null) remaining += path else hold(reason, path)
        }

        remaining.sorted().forEach { path ->
            confirmationHold(path, statistics, history, listingTime)?.let { hold(it, path) }
        }

        return StorageHolds(held.toSortedMap().mapValues { (_, paths) -> paths.sorted() }, history)
    }

    private fun confirmationHold(
        path: String,
        statistics: CandidateFolderStatistics,
        history: AuditHistory,
        listingTime: Instant,
    ): StorageHoldReason? {
        val fingerprint = statistics.fingerprintsByFolder[path] ?: return StorageHoldReason.INSPECTION_INCOMPLETE
        return when (history) {
            AuditHistory.NotRead -> StorageHoldReason.INSPECTION_INCOMPLETE
            AuditHistory.NoJobIdentity -> StorageHoldReason.CONFIRMATION_NO_JOB_ID
            is AuditHistory.Unreadable -> StorageHoldReason.CONFIRMATION_HISTORY_UNREADABLE
            is AuditHistory.Read ->
                when {
                    // Rows store paths made safe, which may change or cut a path; such a path cannot be matched exactly.
                    SafeText.path(path) != path -> StorageHoldReason.CONFIRMATION_PATH_NOT_EXACT
                    history.confirms(path, fingerprint.value, listingTime) -> null
                    else -> StorageHoldReason.AWAITING_CONFIRMATION
                }
        }
    }

    /**
     * Read whenever a folder was inspected, in dry runs too, so every row that records fingerprints also names the row
     * before it and keeps the chain that confirmation follows.
     */
    fun readHistory(
        catalog: String,
        database: String,
        statistics: CandidateFolderStatistics,
        listingTime: Instant,
    ): AuditHistory {
        if (statistics.fingerprintsByFolder.isEmpty()) {
            return AuditHistory.NotRead
        }
        val jobId = cleanupAuditTableService.currentExternalJobId() ?: return AuditHistory.NoJobIdentity
        return try {
            AuditHistory.Read(
                cleanupAuditTableService.readHistory(
                    externalJobId = SafeText.path(jobId),
                    catalogName = SafeText.path(catalog),
                    databaseName = SafeText.path(database),
                    since = listingTime.minus(HISTORY_LOOKBACK),
                ),
            )
        } catch (th: AuditHistoryUnavailableException) {
            logger.warn("Cannot confirm any folder for catalog=$catalog, database=$database: ${th.message}", th.cause)
            AuditHistory.Unreadable(th.message ?: "unreadable")
        }
    }
}
