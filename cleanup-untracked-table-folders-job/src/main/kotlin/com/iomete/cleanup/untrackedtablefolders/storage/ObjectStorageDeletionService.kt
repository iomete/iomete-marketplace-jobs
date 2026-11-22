package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException
import java.io.FileNotFoundException

/**
 * A folder that was neither deleted nor kept was not found at deletion. [keptDirectories] are empty directories of a
 * deleted folder that are still there.
 */
data class DeletedStorageFolder(
    val path: String,
    val deleted: Boolean,
    val keptReason: FenceKeepReason? = null,
    val keptDirectories: List<String> = emptyList(),
)

enum class DirectoryCleanupOutcome(val code: String) {
    /** Not every listed object was deleted, so no directory was looked at. */
    NOT_RUN("not_run"),
    FINISHED("finished"),

    /** A listing, status read or delete of a directory did not finish, failed or was not sent, so the step stopped. */
    NOT_FINISHED("not_finished"),
}

enum class FolderDeletionOutcome {
    /** Every listed object was deleted and nothing new was found in the folder. */
    DELETED,

    /** A listed object was gone or no longer matched the listing, or an object not in the listing was found. */
    PARTIAL_CHANGED,

    /** A status read or delete failed, was refused, or was not waited for to the end. */
    PARTIAL_FAILED,

    /** The fence stopped deletion before the next status read or delete. */
    PARTIAL_STOPPED,

    /** The listing held no object, so nothing was deleted. */
    NOT_FOUND,

    /** A check before the first delete held the folder, so nothing in it was deleted. */
    KEPT,
}

/**
 * Keys are relative to [folderPath]. Every listed key is in exactly one of [deletedKeys] (confirmed deleted),
 * [remainingKeys] (not attempted, or still there after a delete call returned), [unknownKeys] (a delete threw or was
 * not waited for to the end, so it may have happened or may still happen) and [vanishedKeys] (gone before this job
 * deleted it).
 */
data class FolderDeletionResult(
    val folderPath: String,
    val outcome: FolderDeletionOutcome,
    val listedObjectCount: Int,
    val deletedKeys: List<String> = emptyList(),
    val remainingKeys: List<String> = emptyList(),
    val unknownKeys: List<String> = emptyList(),
    val vanishedKeys: List<String> = emptyList(),
    /** Unlisted objects found in the folder after its listed objects were deleted. */
    val leftoverObjectPaths: List<String> = emptyList(),
    /** Directories that are still there: not empty, refused, not reached, or an object holds the same name. */
    val leftoverDirectoryPaths: List<String> = emptyList(),
    /** Directory deletes that threw or were not waited for to the end, so each may still complete. */
    val unknownDirectoryPaths: List<String> = emptyList(),
    val directoryCleanup: DirectoryCleanupOutcome = DirectoryCleanupOutcome.NOT_RUN,
    val failure: Throwable? = null,
    val stop: FenceStop? = null,
    val keptReason: FenceKeepReason? = null,
    /**
     * Why the removal of empty directories stopped. [stop] carries it only for PARTIAL_STOPPED: a folder with an
     * unlisted object is reported as changed, a failure, and this names the stop in [describe].
     */
    val cleanupStop: FenceStop? = null,
) {
    fun mayHaveDeleted(): Boolean = deletedKeys.isNotEmpty() || unknownKeys.isNotEmpty() || unknownDirectoryPaths.isNotEmpty()

    fun touchedObjects(): Boolean = mayHaveDeleted() || vanishedKeys.isNotEmpty()

    fun describe(): String {
        val what =
            when (outcome) {
                FolderDeletionOutcome.DELETED -> "folder $folderPath was deleted"
                FolderDeletionOutcome.PARTIAL_CHANGED -> "folder $folderPath changed during deletion"
                FolderDeletionOutcome.PARTIAL_FAILED -> "deleting an object of folder $folderPath failed"
                FolderDeletionOutcome.PARTIAL_STOPPED -> "deletion of folder $folderPath was stopped"
                FolderDeletionOutcome.NOT_FOUND -> "folder $folderPath had no object to delete"
                FolderDeletionOutcome.KEPT -> "folder $folderPath was kept before any delete"
            }
        val unknownNote = if (unknownKeys.isNotEmpty()) " (a delete may still complete)" else ""
        // PartialDeletionException adds the message of [stop] itself.
        val cleanupStopNote = cleanupStop?.takeIf { it != stop }?.let { " (${it.message})" }.orEmpty()
        val directories =
            when {
                directoryCleanup != DirectoryCleanupOutcome.NOT_FINISHED -> ""
                unknownDirectoryPaths.isEmpty() -> "; removing its empty directories did not finish$cleanupStopNote"
                else -> "; removing its empty directories did not finish, ${unknownDirectoryPaths.size} directory delete(s) may still complete"
            }
        return "$what: ${deletedKeys.size} of $listedObjectCount listed object(s) deleted, ${remainingKeys.size} kept, " +
            "${unknownKeys.size} in an unknown state$unknownNote, ${vanishedKeys.size} already gone, " +
            "${leftoverObjectPaths.size} unlisted object(s) found$directories"
    }
}

/** With [stop], the fence ended the batch and [failedPath] is null; without it, [failedPath] is where deletion failed. */
class PartialDeletionException(
    val deletedPaths: List<String>,
    val failedPath: String?,
    cause: Throwable?,
    val notFoundPaths: List<String> = emptyList(),
    val folderResult: FolderDeletionResult? = null,
    val keptFolders: List<DeletedStorageFolder> = emptyList(),
    val notAttemptedPaths: List<String> = emptyList(),
    val stop: FenceStop? = null,
    val keptDirectoryPaths: List<String> = emptyList(),
) : IllegalStateException(
    // The cause's message is not copied here: this message reaches the audit row, and the cause can hold connection
    // details. The cause stays attached for the log.
    "Folder deletion stopped after ${deletedPaths.size} confirmed deletion(s)" +
        when {
            folderResult != null -> "; ${folderResult.describe()}"
            failedPath != null -> "; no object of $failedPath was deleted"
            else -> ""
        } + stop?.let { "; ${it.message}" }.orEmpty(),
    cause,
) {
    fun fenceReport(): DeletionFenceReport =
        DeletionFenceReport(
            keptPaths = keptFolders.groupBy({ it.keptReason!! }, { it.path }),
            partialFolder = folderResult?.takeIf { it.touchedObjects() },
            notAttemptedPaths = notAttemptedPaths,
            stop = stop,
            keptDirectoryPaths = keptDirectoryPaths,
        )
}

private class FolderDeletionFailure(
    val path: String?,
    val cause: Throwable?,
    val folderResult: FolderDeletionResult? = null,
    val stop: FenceStop? = null,
)

@ApplicationScoped
class ObjectStorageDeletionService {
    private val logger = SafeLogger.getLogger(ObjectStorageDeletionService::class.java)

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    /**
     * Lists each folder again and deletes exactly the listed objects. A folder the fence keeps, or one above the object
     * budget, is skipped. A folder that is not fully deleted, or a stop from the fence, ends the batch.
     */
    fun deleteFolders(
        catalog: String,
        locations: List<String>,
        fence: FolderDeletionFence,
    ): List<DeletedStorageFolder> {
        if (locations.isEmpty()) {
            return emptyList()
        }

        // Keep partial deletion state outside withFileSystem because operation failures are wrapped.
        val results = mutableListOf<DeletedStorageFolder>()
        var failure: FolderDeletionFailure? = null

        try {
            catalogFileSystemProvider.withFileSystem(
                catalog = catalog,
                operation = "delete storage folder objects",
                locations = locations,
            ) { fileSystem ->
                for (location in locations) {
                    val folderFailure = deleteFolder(catalog, fileSystem, location, results, fence)
                    if (folderFailure != null) {
                        failure = folderFailure
                        break
                    }
                }
            }
        } catch (th: Throwable) {
            if (results.isEmpty() && failure == null) {
                throw th
            }
            failure = failure ?: FolderDeletionFailure(path = null, cause = th)
        }

        failure?.let {
            val failedPath = it.path.takeIf { _ -> it.stop == null }
            val touchedPath = it.folderResult?.takeIf { result -> result.touchedObjects() }?.folderPath
            throw PartialDeletionException(
                deletedPaths = results.filter { result -> result.deleted }.map { result -> result.path },
                failedPath = failedPath,
                cause = it.cause,
                notFoundPaths = results.filter { result -> !result.deleted && result.keptReason == null }.map { result -> result.path },
                folderResult = it.folderResult,
                keptFolders = results.filter { result -> result.keptReason != null },
                notAttemptedPaths =
                    locations.filter { location ->
                        location != failedPath && location != touchedPath && results.none { result -> result.path == location }
                    },
                stop = it.stop,
                keptDirectoryPaths = results.flatMap { result -> result.keptDirectories },
            )
        }

        return results
    }

    private fun deleteFolder(
        catalog: String,
        fileSystem: FileSystem,
        location: String,
        results: MutableList<DeletedStorageFolder>,
        fence: FolderDeletionFence,
    ): FolderDeletionFailure? {
        fun keep(reason: FenceKeepReason): FolderDeletionFailure? {
            logger.warn("Keeping storage folder, nothing deleted: reason=folder_kept_${reason.code}, location=$location")
            results += DeletedStorageFolder(path = location, deleted = false, keptReason = reason)
            return null
        }

        when (val decision = fence.beforeListing(location)) {
            FenceDecision.Proceed -> Unit
            is FenceDecision.Keep -> return keep(decision.reason)
            is FenceDecision.Stop -> return FolderDeletionFailure(path = location, cause = decision.stop.error, stop = decision.stop)
        }

        val listing =
            when (val read = read(catalog, fence, "list storage folder before deletion", location) { FolderObjectListing.list(fileSystem, location) }) {
                is BoundedCall.Done -> read.value
                is BoundedCall.TimedOut -> {
                    val stop = fence.readTimedOut(read.error)
                    return FolderDeletionFailure(path = location, cause = stop.error, stop = stop)
                }
                is BoundedCall.Failed -> {
                    val th = read.error as? CatalogStorageOperationException ?: throw read.error
                    if (th.cause !is FileNotFoundException) {
                        return FolderDeletionFailure(path = location, cause = th)
                    }
                    logger.warn("Storage folder does not exist, skipping delete: location=$location")
                    results += DeletedStorageFolder(path = location, deleted = false)
                    return null
                }
            }

        val listed =
            when (listing) {
                is FolderListing.Listed -> listing
                is FolderListing.TooManyObjects -> return keep(FenceKeepReason.TOO_MANY_OBJECTS)
            }

        if (listed.objects.isNotEmpty()) {
            when (val decision = fence.checkListing(listed)) {
                FenceDecision.Proceed -> Unit
                is FenceDecision.Keep -> return keep(decision.reason)
                is FenceDecision.Stop -> return FolderDeletionFailure(path = location, cause = decision.stop.error, stop = decision.stop)
            }
        }

        val result = deleteListedObjects(catalog, fileSystem, listed, fence)
        result.stop?.let {
            return FolderDeletionFailure(path = location, cause = it.error, folderResult = result, stop = it)
        }
        return when (result.outcome) {
            FolderDeletionOutcome.DELETED -> {
                results += DeletedStorageFolder(path = location, deleted = true, keptDirectories = result.leftoverDirectoryPaths)
                null
            }

            FolderDeletionOutcome.NOT_FOUND -> {
                results += DeletedStorageFolder(path = location, deleted = false)
                null
            }

            FolderDeletionOutcome.KEPT -> keep(checkNotNull(result.keptReason))

            else ->
                FolderDeletionFailure(
                    path = location,
                    cause = result.failure ?: IllegalStateException(result.describe()),
                    folderResult = result,
                )
        }
    }

    /**
     * Deletes exactly the objects of [listing], one at a time with a non-recursive delete. First every listed key is
     * read back under its own name, so a folder whose keys cannot all be addressed is held before anything in it is
     * deleted. Then, per key: the fence check, a status read that must still match the listing, the fence check before
     * a delete, and the delete. Stops the folder at the first change, failure, uncertain delete or fence stop. Once
     * every listed object is gone, removes the folder's empty directories bottom-up, again without recursion. That
     * step stops at its first directory call that fails, is not waited for to the end, or is not sent, and at a
     * directory listing that returns an object of the directory's name. The folder then counts as partly deleted,
     * because an object written after the listing may be in a directory it did not list.
     *
     * Residual races: an object overwritten after its last status read and before its delete is deleted with its new
     * content, and so is one that a storage client retry of that delete reaches. The storage client reads the
     * object's status and may retry inside the delete call, so the request itself can go out at any time until the
     * call returns or the job stops waiting. A delete the job stopped waiting for may still complete later, with no
     * bound. An object written at a directory's exact name after that directory's status read is deleted by the
     * directory delete; for the folder itself that object is outside the folder.
     */
    fun deleteListedObjects(
        catalog: String,
        fileSystem: FileSystem,
        listing: FolderListing.Listed,
        fence: FolderDeletionFence,
        maxCleanupEntries: Int = MAX_OBJECTS_PER_FOLDER,
    ): FolderDeletionResult {
        val folderPath = listing.folderPath
        val objects = listing.objects.sortedBy { it.relativeKey }
        if (objects.isEmpty()) {
            logger.warn("Storage folder holds no object at deletion, nothing deleted: location=$folderPath")
            return FolderDeletionResult(folderPath, FolderDeletionOutcome.NOT_FOUND, listedObjectCount = 0)
        }

        val folder = FolderObjectListing.folderPrefix(folderPath)
        val deleted = mutableListOf<String>()

        fun stop(
            outcome: FolderDeletionOutcome,
            unknown: String? = null,
            vanished: String? = null,
            failure: Throwable? = null,
            stop: FenceStop? = null,
            keptReason: FenceKeepReason? = null,
        ): FolderDeletionResult {
            val settled = deleted.toSet() + listOfNotNull(unknown, vanished)
            return FolderDeletionResult(
                folderPath = folderPath,
                outcome = outcome,
                listedObjectCount = objects.size,
                deletedKeys = deleted.toList(),
                remainingKeys = objects.map { it.relativeKey }.filterNot { it in settled },
                unknownKeys = listOfNotNull(unknown),
                vanishedKeys = listOfNotNull(vanished),
                failure = failure,
                stop = stop,
                keptReason = keptReason,
            ).also { logger.warn("Stopped deleting storage folder: ${it.describe()}", failure) }
        }

        // Hadoop's Path rewrites "//", "." and ".." in a key, so such a listed key names another object or none.
        // Reading every key back before the first delete holds such a folder instead of leaving it partly deleted.
        if (objects.distinctBy { it.relativeKey }.size != objects.size) {
            return stop(FolderDeletionOutcome.KEPT, keptReason = FenceKeepReason.KEY_NOT_ADDRESSABLE)
        }
        val paths = ArrayList<Path>(objects.size)
        for (listed in objects) {
            fence.beforeNextObject()?.let { return stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = it) }
            val path =
                try {
                    FolderObjectListing.objectPath(folder, listed.relativeKey)
                } catch (th: IllegalStateException) {
                    return stop(FolderDeletionOutcome.PARTIAL_FAILED, failure = th)
                }
            when (val status = status(catalog, fileSystem, path, fence)) {
                is StatusRead.Found ->
                    if (!stillMatches(listed, status.status)) {
                        return stop(FolderDeletionOutcome.PARTIAL_CHANGED)
                    }
                StatusRead.Missing -> return stop(FolderDeletionOutcome.KEPT, keptReason = FenceKeepReason.KEY_NOT_ADDRESSABLE)
                is StatusRead.Failed -> return stop(FolderDeletionOutcome.PARTIAL_FAILED, failure = status.error)
                is StatusRead.TimedOut -> return stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = fence.readTimedOut(status.error))
            }
            paths += path
        }

        logger.warn("Deleting ${objects.size} listed object(s) of storage folder: catalog=$catalog, location=$folderPath")

        objects.forEachIndexed { index, listed ->
            val key = listed.relativeKey
            val path = paths[index]
            fence.beforeNextObject()?.let { return stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = it) }

            val current =
                when (val status = status(catalog, fileSystem, path, fence)) {
                    is StatusRead.Found -> status.status
                    StatusRead.Missing -> return stop(FolderDeletionOutcome.PARTIAL_CHANGED, vanished = key)
                    is StatusRead.Failed -> return stop(FolderDeletionOutcome.PARTIAL_FAILED, failure = status.error)
                    is StatusRead.TimedOut -> return stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = fence.readTimedOut(status.error))
                }

            if (!stillMatches(listed, current)) {
                return stop(FolderDeletionOutcome.PARTIAL_CHANGED)
            }

            // Again right before the delete, so a slow status read cannot let a delete go out with an old snapshot.
            fence.stopBeforeDelete()?.let { return stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = it) }

            // A delete that threw or was not waited for may have happened or may still happen, so it is never sent
            // again and the key stays unknown. One that was never sent leaves its key remaining.
            val removed =
                when (val call = deleteCall(catalog, fence, "delete storage object", path.toString()) { fileSystem.delete(path, false) }) {
                    is BoundedCall.Done -> call.value
                    is BoundedCall.TimedOut ->
                        return if (call.error.started) {
                            stop(FolderDeletionOutcome.PARTIAL_FAILED, unknown = key, failure = call.error)
                        } else {
                            stop(FolderDeletionOutcome.PARTIAL_STOPPED, stop = notSent(fence, call.error))
                        }
                    is BoundedCall.Failed -> {
                        val th = call.error as? CatalogStorageOperationException ?: throw call.error
                        return stop(FolderDeletionOutcome.PARTIAL_FAILED, unknown = key, failure = th)
                    }
                }

            if (!removed) {
                // The delete call has returned, so a status read now shows what it left.
                return when (val after = status(catalog, fileSystem, path, fence)) {
                    is StatusRead.Found -> stop(FolderDeletionOutcome.PARTIAL_FAILED)
                    StatusRead.Missing -> stop(FolderDeletionOutcome.PARTIAL_CHANGED, vanished = key)
                    is StatusRead.Failed -> stop(FolderDeletionOutcome.PARTIAL_FAILED, unknown = key, failure = after.error)
                    is StatusRead.TimedOut -> stop(FolderDeletionOutcome.PARTIAL_FAILED, unknown = key, failure = after.error)
                }
            }

            deleted += key
            if (logger.isDebugEnabled) {
                logger.debug("Deleted storage object: location=$path")
            }
        }

        val cleanup =
            DirectoryCleanup(catalog, fileSystem, folder, maxCleanupEntries, fence).also {
                if (!it.removeEmpty(Path(folder))) it.recheckKeptFolder()
            }
        val outcome =
            when {
                cleanup.leftoverObjects.isNotEmpty() -> FolderDeletionOutcome.PARTIAL_CHANGED
                cleanup.stop != null -> FolderDeletionOutcome.PARTIAL_STOPPED
                cleanup.failure != null -> FolderDeletionOutcome.PARTIAL_FAILED
                else -> FolderDeletionOutcome.DELETED
            }

        return FolderDeletionResult(
            folderPath = folderPath,
            outcome = outcome,
            listedObjectCount = objects.size,
            deletedKeys = deleted.toList(),
            leftoverObjectPaths = cleanup.leftoverObjects.toList(),
            leftoverDirectoryPaths = cleanup.leftoverDirectories.toList(),
            unknownDirectoryPaths = cleanup.unknownDirectories.toList(),
            directoryCleanup = if (cleanup.finished) DirectoryCleanupOutcome.FINISHED else DirectoryCleanupOutcome.NOT_FINISHED,
            failure = cleanup.failure,
            stop = cleanup.stop.takeIf { outcome == FolderDeletionOutcome.PARTIAL_STOPPED },
            cleanupStop = cleanup.stop,
        ).also { result ->
            if (outcome == FolderDeletionOutcome.DELETED) {
                logger.warn("Storage folder deleted: ${result.describe()}")
            } else {
                logger.warn("Stopped deleting storage folder: ${result.describe()}", result.failure ?: result.stop?.error)
            }
            if (result.leftoverDirectoryPaths.isNotEmpty() || result.unknownDirectoryPaths.isNotEmpty()) {
                logger.warn(
                    "Directories left after deleting the objects of folder=$folderPath: kept=${result.leftoverDirectoryPaths}, " +
                        "deleteMayStillComplete=${result.unknownDirectoryPaths}, cleanup=${result.directoryCleanup.code}"
                )
            }
        }
    }

    // A call that was not sent because no time was left: the snapshot reached its limit right before it.
    private fun notSent(
        fence: FolderDeletionFence,
        error: CallTimedOutException,
    ): FenceStop = fence.stopBeforeNextObject() ?: fence.readTimedOut(error)

    // A HEAD reports Last-Modified to the second, while a listing may report milliseconds, so times are compared to
    // the second. Length and, when the listing had one, the ETag must match exactly.
    private fun stillMatches(
        listed: ListedObject,
        current: FileStatus,
    ): Boolean =
        !current.isDirectory &&
            current.len == listed.length &&
            Math.floorDiv(current.modificationTime, 1000L) == Math.floorDiv(listed.modificationTimeMillis, 1000L) &&
            (listed.etag == null || etagOf(current) == listed.etag)

    private fun <T> read(
        catalog: String,
        fence: FolderDeletionFence,
        operation: String,
        location: String,
        block: () -> T,
    ): BoundedCall<T> =
        BoundedCalls.run(operation, "location=$location", fence.readWait()) {
            catalogFileSystemProvider.runOperation(catalog, operation, location, block)
        }

    private fun <T> deleteCall(
        catalog: String,
        fence: FolderDeletionFence,
        operation: String,
        location: String,
        block: () -> T,
    ): BoundedCall<T> =
        BoundedCalls.run(operation, "location=$location", fence.deleteWait()) {
            catalogFileSystemProvider.runOperation(catalog, operation, location, block)
        }

    private sealed interface StatusRead {
        class Found(val status: FileStatus) : StatusRead

        data object Missing : StatusRead

        class Failed(val error: Throwable) : StatusRead

        class TimedOut(val error: CallTimedOutException) : StatusRead
    }

    private fun status(
        catalog: String,
        fileSystem: FileSystem,
        path: Path,
        fence: FolderDeletionFence,
    ): StatusRead =
        when (val read = read(catalog, fence, "read storage object status", path.toString()) { fileSystem.getFileStatus(path) }) {
            is BoundedCall.Done -> StatusRead.Found(read.value)
            is BoundedCall.TimedOut -> StatusRead.TimedOut(read.error)
            is BoundedCall.Failed -> {
                val th = read.error as? CatalogStorageOperationException ?: throw read.error
                if (th.cause is FileNotFoundException) StatusRead.Missing else StatusRead.Failed(th)
            }
        }

    // Visits at most maxEntries listed entries, so a folder full of empty directories cannot run on unbounded. The
    // first stop or failure ends the step, so no later directory is listed or deleted.
    private inner class DirectoryCleanup(
        private val catalog: String,
        private val fileSystem: FileSystem,
        private val folder: String,
        private val maxEntries: Int,
        private val fence: FolderDeletionFence,
    ) {
        val leftoverObjects = linkedSetOf<String>()
        val leftoverDirectories = linkedSetOf<String>()
        val unknownDirectories = linkedSetOf<String>()
        var stop: FenceStop? = null
            private set
        var failure: Throwable? = null
            private set

        val finished: Boolean
            get() = stop == null && failure == null

        private var entriesLeft = maxEntries

        private fun list(dir: String): BoundedCall<Array<FileStatus>> =
            read(catalog, fence, "list storage directory after deletion", dir) { fileSystem.listStatus(Path(dir)) }

        private fun timedOut(error: CallTimedOutException): FenceStop = if (error.started) fence.readTimedOut(error) else notSent(fence, error)

        /** Ends the step at [dir], which is kept unless its delete may still complete. The first stop or failure is reported. */
        private fun halt(
            dir: String,
            stop: FenceStop? = null,
            failure: Throwable? = null,
            unknown: Boolean = false,
        ): Boolean {
            if (unknown) unknownDirectories += dir else leftoverDirectories += dir
            if (finished) {
                this.stop = stop
                this.failure = failure
            }
            return false
        }

        /** Returns true when [directory] no longer exists. */
        fun removeEmpty(directory: Path): Boolean {
            val dir = directory.toString()
            val children =
                when (val read = list(dir)) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return halt(dir, stop = timedOut(read.error))
                    is BoundedCall.Failed -> {
                        val th = read.error as? CatalogStorageOperationException ?: throw read.error
                        if (th.cause is FileNotFoundException) return true
                        return halt(dir, failure = th)
                    }
                }

            var empty = true
            for (child in children.sortedBy { it.path.toString() }) {
                if (!finished) {
                    empty = false
                    break
                }
                if (--entriesLeft < 0) {
                    return halt(dir, failure = IllegalStateException("more than $maxEntries entries were found below folder=$folder after its objects were deleted"))
                }
                val childPath = child.path.toString()
                when {
                    // A listing of an object returns the object itself. S3A does so only when nothing is listed below
                    // the name; other stores may not, so what is below is not known. Below the folder such an object
                    // is new; one named like the folder itself is not the folder's.
                    !childPath.startsWith("$dir/") -> {
                        if (childPath.startsWith("$folder/")) leftoverObjects += childPath
                        return halt(dir, failure = IllegalStateException("the listing of storage directory=$dir returned an object of the same name"))
                    }
                    child.isDirectory -> if (!removeEmpty(child.path)) empty = false
                    else -> {
                        leftoverObjects += childPath
                        empty = false
                    }
                }
            }

            if (!empty) {
                leftoverDirectories += dir
                return false
            }
            return removeDirectory(directory)
        }

        private fun removeDirectory(directory: Path): Boolean {
            val dir = directory.toString()
            fence.beforeNextObject()?.let { return halt(dir, stop = it) }
            // S3A deletes an object named like the directory before its marker, so only a directory status is deleted.
            // One written at that name after this read is still deleted; nothing here can close that window.
            when (val before = status(catalog, fileSystem, directory, fence)) {
                StatusRead.Missing -> return true
                is StatusRead.Failed -> return halt(dir, failure = before.error)
                is StatusRead.TimedOut -> return halt(dir, stop = timedOut(before.error))
                is StatusRead.Found ->
                    if (!before.status.isDirectory) {
                        // Below the folder such an object is new; one named like the folder itself is not the folder's.
                        if (dir != folder) leftoverObjects += dir
                        return keep(dir, "an object has the same name")
                    }
            }

            fence.stopBeforeDelete()?.let { return halt(dir, stop = it) }

            val removed =
                when (val call = deleteCall(catalog, fence, "delete empty storage directory", dir) { fileSystem.delete(directory, false) }) {
                    is BoundedCall.Done -> call.value
                    is BoundedCall.TimedOut -> {
                        if (!call.error.started) return halt(dir, stop = notSent(fence, call.error))
                        logger.warn("The delete of storage directory did not finish in time and may still complete: location=$dir", call.error)
                        return halt(dir, failure = call.error, unknown = true)
                    }
                    is BoundedCall.Failed -> {
                        val th = call.error as? CatalogStorageOperationException ?: throw call.error
                        // S3A refuses a directory that is not empty before it sends any delete request.
                        if (th.cause is PathIsNotEmptyDirectoryException) return keep(dir, "it is not empty", th, recheck = true)
                        return uncertain(dir, th)
                    }
                }

            if (removed) {
                return true
            }
            return keep(dir, "its delete returned false", recheck = true)
        }

        // A delete that threw may still be carried out by the store. A recheck can show it gone, never that it stays.
        private fun uncertain(
            dir: String,
            error: Throwable,
        ): Boolean {
            if (recheck(dir) == Recheck.Gone) {
                return true
            }
            logger.warn("The delete of storage directory failed and may still complete: location=$dir", error)
            return halt(dir, failure = error, unknown = true)
        }

        private fun keep(
            dir: String,
            why: String,
            error: Throwable? = null,
            recheck: Boolean = false,
        ): Boolean {
            when (val after = if (recheck) recheck(dir) else Recheck.Present) {
                Recheck.Gone -> return true
                Recheck.Present -> Unit
                is Recheck.Unknown -> return halt(dir, stop = after.stop, failure = after.failure)
            }
            logger.warn("Keeping storage directory because $why: location=$dir", error)
            leftoverDirectories += dir
            return false
        }

        // A kept folder is not rechecked at its own delete: a kept directory keeps every directory above it, and an
        // object at the folder's name keeps it with no delete. So an object written below a directory removed earlier
        // would not be seen. When the step finished and found nothing new, the kept folder is listed once more. Every
        // object below it is new; a kept directory is not, and is not counted again.
        fun recheckKeptFolder() {
            if (!finished || leftoverObjects.isNotEmpty()) return
            unlistedBelow(folder)?.let { halt(folder, stop = it.stop, failure = it.failure) }
        }

        // After a refused or failed delete, what is still there. Unlisted objects are recorded, also those below a
        // directory found here: every directory below was removed or empty before this delete, so what it holds is new.
        private fun recheck(dir: String): Recheck {
            val children =
                when (val read = list(dir)) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return Recheck.Unknown(stop = timedOut(read.error))
                    is BoundedCall.Failed -> {
                        val th = read.error as? CatalogStorageOperationException ?: throw read.error
                        if (th.cause is FileNotFoundException) return Recheck.Gone
                        return Recheck.Unknown(failure = th)
                    }
                }
            recordAndWalk(dir, children)?.let { return it }
            if (children.isNotEmpty()) {
                return Recheck.Present
            }
            return when (val after = status(catalog, fileSystem, Path(dir), fence)) {
                StatusRead.Missing -> Recheck.Gone
                is StatusRead.Found -> Recheck.Present
                is StatusRead.Failed -> Recheck.Unknown(failure = after.error)
                is StatusRead.TimedOut -> Recheck.Unknown(stop = timedOut(after.error))
            }
        }

        // Records every object below [dir], and an empty directory as itself, within the step's entry limit. Null when
        // the walk finished; otherwise why it could not, so the folder is never reported as deleted.
        private fun unlistedBelow(dir: String): Recheck.Unknown? {
            val children =
                when (val read = list(dir)) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return Recheck.Unknown(stop = timedOut(read.error))
                    is BoundedCall.Failed -> {
                        val th = read.error as? CatalogStorageOperationException ?: throw read.error
                        if (th.cause is FileNotFoundException) return null
                        return Recheck.Unknown(failure = th)
                    }
                }
            if (children.isEmpty()) {
                if (dir.startsWith("$folder/") && dir !in leftoverDirectories) leftoverObjects += dir
                return null
            }
            return recordAndWalk(dir, children)
        }

        // Objects first, so each one listed here is named even when what follows cannot finish. Every entry counts
        // toward the step's entry limit. An object named like the folder itself is not the folder's.
        private fun recordAndWalk(
            dir: String,
            children: Array<FileStatus>,
        ): Recheck.Unknown? {
            val sorted = children.sortedBy { it.path.toString() }
            for (child in sorted) {
                val childPath = child.path.toString()
                if (!child.isDirectory && childPath.startsWith("$folder/")) leftoverObjects += childPath
            }
            if (sorted.any { !it.path.toString().startsWith("$dir/") }) {
                return Recheck.Unknown(failure = IllegalStateException("the listing of storage directory=$dir returned an object of the same name"))
            }
            entriesLeft -= sorted.count { it.path.toString() !in leftoverDirectories }
            if (entriesLeft < 0) {
                return Recheck.Unknown(failure = IllegalStateException("more than $maxEntries entries were found below folder=$folder after its objects were deleted"))
            }
            for (child in sorted) {
                if (child.isDirectory) unlistedBelow(child.path.toString())?.let { return it }
            }
            return null
        }
    }

    private sealed interface Recheck {
        data object Gone : Recheck

        data object Present : Recheck

        class Unknown(
            val stop: FenceStop? = null,
            val failure: Throwable? = null,
        ) : Recheck
    }
}
