package com.iomete.cleanup.untrackedtablefolders.storage

import java.time.Duration

/**
 * How old the ownership snapshot may be while a delete runs, measured from the start of the read on a monotonic
 * clock. The snapshot is read again once less than twice [DELETE_START_MARGIN] of this age is left, before a folder
 * or between the objects and directories of one, and deletion stops when a read takes more than half this time.
 * No delete call starts once the snapshot is within [DELETE_START_MARGIN] of this age, and each read and
 * delete is waited for only until the snapshot reaches it. Inside a delete call the storage client reads the object's
 * status and may retry before and after it sends the request, so the request can go out at any time until the call
 * returns or the wait ends. A delete the job stopped waiting for may still land later, with no bound on when.
 */
val OWNERSHIP_SNAPSHOT_MAX_AGE: Duration = Duration.ofMinutes(5)

/**
 * A delete starts only while at least this much of the snapshot's age limit is left, or a tenth of the limit when
 * that is shorter. Without it the limit usually falls inside a delete call, whose request may then land after it. A
 * call can still take longer than this, for example while the storage client retries, so it is not a bound.
 */
val DELETE_START_MARGIN: Duration = Duration.ofSeconds(30)

/**
 * The checks the deletion service runs for each folder, in order: before listing it, on the listing, before each
 * status read and right before each delete. It also sets how long the service waits for each storage call.
 */
interface FolderDeletionFence {
    fun beforeListing(folderPath: String): FenceDecision = FenceDecision.Proceed

    fun checkListing(listing: FolderListing.Listed): FenceDecision = FenceDecision.Proceed

    fun stopBeforeNextObject(): FenceStop? = null

    /**
     * Called before each object's status read and before each directory's. Unlike [stopBeforeNextObject], it may read
     * ownership again and check the folder against that read, so a folder can go on past the age of one read.
     */
    fun beforeNextObject(): FenceStop? = stopBeforeNextObject()

    /** Checked right before each object or directory delete is sent. */
    fun stopBeforeDelete(): FenceStop? = stopBeforeNextObject()

    /** How long a storage read may be waited for now. Null runs it on the caller's thread with no limit. */
    fun readWait(): Duration? = null

    /** How long one delete may be waited for. Null runs it on the caller's thread with no limit. */
    fun deleteWait(): Duration? = null

    fun readTimedOut(error: CallTimedOutException): FenceStop =
        FenceStop(
            FenceStopCause.READ_TIMED_OUT,
            if (error.started) "the job stopped waiting to ${error.operation} for ${error.target}" else "the job did not ${error.operation} for ${error.target}, because no time was left",
            error,
        )

    companion object {
        /** Checks nothing. Only tests of the storage layer pass it; candidate deletion goes through the gate's fence. */
        val NONE: FolderDeletionFence = object : FolderDeletionFence {}
    }
}

sealed interface FenceDecision {
    data object Proceed : FenceDecision

    data class Keep(val reason: FenceKeepReason) : FenceDecision

    data class Stop(val stop: FenceStop) : FenceDecision
}

/** Why the fence kept one folder. The audit key is folder_kept_<code>. */
enum class FenceKeepReason(val code: String) {
    /** The folder overlaps storage that a read of this run protects. */
    CLAIMED("at_final_check"),
    TABLE_NAMED_LIKE_FOLDER("table_named_like_folder"),
    CHANGED_SINCE_INSPECTION("changed_since_inspection"),
    TOO_MANY_OBJECTS("too_many_objects"),
    TOO_NEW("too_new"),
    UNKNOWN_AGE("unknown_age"),

    /** Before the first delete, a listed key was not found under its own name, or two listed keys had one name. */
    KEY_NOT_ADDRESSABLE("key_not_addressable"),
}

/** Why the fence stopped the remaining folders of a database. */
enum class FenceStopCause(val code: String) {
    OTHER_STORAGE_INCOMPLETE("other_storage_incomplete"),
    OWN_TABLES_UNRESOLVED("own_tables_unresolved"),
    OWN_DATABASE_RECHECK_FAILED("own_database_recheck_failed"),
    RECHECK_TOO_SLOW("recheck_too_slow"),
    NAME_PROBE_FAILED("name_probe_failed"),

    /** Ownership read again in the middle of a folder protects that folder; the objects deleted before stay deleted. */
    CLAIMED_DURING_DELETION("claimed_during_deletion"),

    /** The snapshot passed its age limit during deletion, or was too close to it to start another delete. */
    SNAPSHOT_EXPIRED("snapshot_expired_during_deletion"),

    /** A read before a delete was still running when the snapshot reached its age limit. */
    READ_TIMED_OUT("read_timed_out"),
}

/** [message] is built from paths and counts only, because it reaches the audit row; [error] stays for the log. */
data class FenceStop(
    val cause: FenceStopCause,
    val message: String,
    val error: Throwable? = null,
)

/**
 * One read of the storage of other databases, catalogs and tables. Paths are null when any of it could not be read.
 * The read runs on a worker thread that may outlive the gate's wait, so it records nothing itself: the gate calls
 * [accept] on its own thread for each read it uses.
 */
class OtherStorageRecheck(
    val paths: List<String>?,
    val unresolvedTables: List<String>,
    private val onAccepted: () -> Unit = {},
) {
    val complete: Boolean
        get() = paths != null && unresolvedTables.isEmpty()

    fun accept() = onAccepted()
}

/** What the run learned before deletion: the inspection fingerprints, the age cutoff, and how to read other storage again. */
class DeletionFence(
    val inspectionFingerprints: Map<String, FolderFingerprint>,
    val cutoffTimeMillis: Long,
    val readOtherStorage: () -> OtherStorageRecheck,
)

data class DeletionFenceReport(
    val keptPaths: Map<FenceKeepReason, List<String>> = emptyMap(),
    /** The folder where deletion ended with some of its listed objects deleted, in an unknown state, or gone. */
    val partialFolder: FolderDeletionResult? = null,
    val notAttemptedPaths: List<String> = emptyList(),
    val stop: FenceStop? = null,
    val ownershipRefreshCount: Int = 0,
    /** Empty directories still there in folders that were deleted. */
    val keptDirectoryPaths: List<String> = emptyList(),
)
