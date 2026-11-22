package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Duration

data class CandidateDeletionResult(
    val deletedPaths: List<String> = emptyList(),
    val notFoundPaths: List<String> = emptyList(),
    val fence: DeletionFenceReport = DeletionFenceReport(),
) {
    val deletedAnything: Boolean
        get() = deletedPaths.isNotEmpty() || fence.partialFolder?.mayHaveDeleted() == true
}

@ApplicationScoped
class CandidateDeletionGate {

    private val logger = SafeLogger.getLogger(CandidateDeletionGate::class.java)

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var catalogDiscoveryService: CatalogDiscoveryService
    @Inject lateinit var objectStorageDeletionService: ObjectStorageDeletionService

    // Elapsed time comes from a monotonic clock, so a wall clock step cannot stretch the snapshot limit.
    var ticker: Ticker = Ticker.SYSTEM
    var snapshotMaxAge: Duration = OWNERSHIP_SNAPSHOT_MAX_AGE

    /** Null means [DELETE_START_MARGIN], or a tenth of [snapshotMaxAge] when that is shorter. */
    var deleteStartMargin: Duration? = null

    /**
     * Deletes the candidates one folder at a time behind a fence. The ownership snapshot is [fence]'s read of other
     * storage plus a new read of this database; folders are checked against it, and it is read again before a folder
     * only once it is older than [snapshotMaxAge], so an owner registered after the last read is not seen. Every read
     * only adds to the protected paths, which start as [excludedPaths].
     * A snapshot that cannot be read, is incomplete or is not read within [snapshotMaxAge] stops the remaining
     * folders, and so does a snapshot that grows too old while a folder is being deleted. Each catalog read, storage
     * read and delete of a folder is waited for only until the snapshot is [snapshotMaxAge] old. No read starts after
     * that, and no delete starts once less than the start margin is left. Only the wait is bounded: a call the job
     * stopped waiting for may still finish, and a delete may still land, with no bound on when.
     */
    fun deleteCandidates(
        catalog: String,
        database: String,
        reconciliation: StorageFolderReconciliation.DeletionEligible,
        excludedPaths: List<String>,
        fence: DeletionFence,
    ): CandidateDeletionResult {
        if (config.dryRun) {
            return CandidateDeletionResult()
        }

        check(config.deleteEnabled) {
            "delete_enabled must be true before deleting candidate folders"
        }

        if (reconciliation.folders.isEmpty()) {
            return CandidateDeletionResult()
        }

        val folderPaths = reconciliation.folders.map { it.path }.sorted()
        val folderFence = Fence(catalog, database, fence, excludedPaths)
        folderFence.readSnapshot(beforeFolder = null)?.let { stop ->
            return CandidateDeletionResult(fence = DeletionFenceReport(notAttemptedPaths = folderPaths, stop = stop))
        }

        val results =
            try {
                objectStorageDeletionService.deleteFolders(catalog, folderPaths, folderFence)
            } catch (th: PartialDeletionException) {
                if (th.stop == null) {
                    throw th
                }
                return CandidateDeletionResult(
                    deletedPaths = th.deletedPaths.sorted(),
                    notFoundPaths = th.notFoundPaths.sorted(),
                    fence = th.fenceReport(),
                )
            }

        return CandidateDeletionResult(
            deletedPaths = results.filter { it.deleted }.map { it.path }.sorted(),
            notFoundPaths = results.filter { !it.deleted && it.keptReason == null }.map { it.path }.sorted(),
            fence =
                DeletionFenceReport(
                    keptPaths = results.filter { it.keptReason != null }.groupBy({ it.keptReason!! }, { it.path }),
                    keptDirectoryPaths = results.flatMap { it.keptDirectories },
                ),
        )
    }

    private inner class Fence(
        private val catalog: String,
        private val database: String,
        private val fence: DeletionFence,
        excludedPaths: List<String>,
    ) : FolderDeletionFence {
        private val protectedPaths = excludedPaths.map(StoragePathUtils::normalizeLocation).toMutableSet()
        private val inspectionFingerprints = fence.inspectionFingerprints.mapKeys { StoragePathUtils.normalizeLocation(it.key) }
        private val maxAgeNanos = snapshotMaxAge.toNanos()
        private val startMargin = deleteStartMargin ?: minOf(DELETE_START_MARGIN, snapshotMaxAge.dividedBy(10))
        private var snapshotStartedAt: Long? = null

        private fun snapshotAgeNanos(): Long = snapshotStartedAt?.let { ticker.nanos() - it } ?: Long.MAX_VALUE

        private fun text(duration: Duration): String = if (duration.toMillis() % 1000 == 0L) "${duration.seconds} s" else "${duration.toMillis()} ms"

        private fun limitText(): String = text(snapshotMaxAge)

        // A read runs on a worker and is waited for only until [deadline]; the value of a read that timed out is dropped.
        private fun <T> read(
            operation: String,
            target: String,
            deadline: Long,
            block: () -> T,
        ): BoundedCall<T> = BoundedCalls.run(operation, target, Duration.ofNanos(deadline - ticker.nanos()), block)

        /** Returns why deletion must stop, or null when the snapshot was read in full within the time limit. */
        fun readSnapshot(beforeFolder: String?): FenceStop? {
            val startedAt = ticker.nanos()
            val deadline = startedAt + maxAgeNanos
            val where = beforeFolder?.let { " before folder=$it" }.orEmpty()

            fun tooSlow(timedOut: CallTimedOutException?): FenceStop =
                FenceStop(
                    FenceStopCause.RECHECK_TOO_SLOW,
                    if (timedOut?.started == true) {
                        "the ownership recheck$where did not finish within the ${limitText()} limit"
                    } else {
                        "the ownership recheck$where took ${Duration.ofNanos(ticker.nanos() - startedAt).seconds} s, longer than the ${limitText()} limit"
                    },
                    timedOut,
                )

            val otherStorage =
                when (val read = read("read the storage of other databases", "catalog=$catalog", deadline) { fence.readOtherStorage() }) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return tooSlow(read.error)
                    is BoundedCall.Failed -> {
                        val th = read.error as? Exception ?: throw read.error
                        return FenceStop(FenceStopCause.OTHER_STORAGE_INCOMPLETE, "the storage of other databases could not be read again$where", th)
                    }
                }
            otherStorage.accept()
            protectedPaths += otherStorage.paths.orEmpty().map(StoragePathUtils::normalizeLocation)
            if (!otherStorage.complete) {
                return FenceStop(
                    FenceStopCause.OTHER_STORAGE_INCOMPLETE,
                    "the storage of other databases, catalogs or tables could not be established at the recheck$where",
                )
            }

            val discoveredDatabase =
                when (
                    val read =
                        read("read the tables of this database", "catalog=$catalog, database=$database", deadline) {
                            catalogDiscoveryService.discoverDatabase(catalog = catalog, database = database)
                        }
                ) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return tooSlow(read.error)
                    is BoundedCall.Failed -> {
                        val th = read.error as? Exception ?: throw read.error
                        return FenceStop(FenceStopCause.OWN_DATABASE_RECHECK_FAILED, "the tables of catalog=$catalog, database=$database could not be read again$where", th)
                    }
                }
            protectedPaths +=
                discoveredDatabase.tables
                    .flatMap { listOfNotNull(it.location) + it.writeLocations }
                    .map(StoragePathUtils::normalizeLocation)
            // A table that became unresolved leaves an unknown location behind, so no folder can still be proven untracked.
            if (discoveredDatabase.unresolvedTables.isNotEmpty()) {
                return FenceStop(
                    FenceStopCause.OWN_TABLES_UNRESOLVED,
                    "Refusing to delete: ${discoveredDatabase.unresolvedTables.size} catalog table(s) could not be resolved to a " +
                        "storage location during the pre-delete recheck$where. catalog=$catalog, database=$database",
                )
            }

            if (ticker.nanos() - startedAt > maxAgeNanos) {
                return tooSlow(null)
            }
            snapshotStartedAt = startedAt
            return null
        }

        override fun beforeListing(folderPath: String): FenceDecision {
            if (snapshotAgeNanos() > maxAgeNanos) {
                logger.info("Reading ownership again before folder=$folderPath: the snapshot is older than ${limitText()}")
                readSnapshot(beforeFolder = folderPath)?.let { return FenceDecision.Stop(it) }
            }

            val folder = StoragePathUtils.normalizeLocation(folderPath)
            val name = folder.substringAfterLast('/')
            val namedTable =
                when (
                    val read =
                        read("look up the table named like the folder", "catalog=$catalog, database=$database, table=$name", snapshotDeadline()) {
                            catalogDiscoveryService.tableStorageIfExists(catalog, database, name)
                        }
                ) {
                    is BoundedCall.Done -> read.value
                    is BoundedCall.TimedOut -> return FenceDecision.Stop(readTimedOut(read.error))
                    is BoundedCall.Failed -> {
                        val th = read.error as? Exception ?: throw read.error
                        return FenceDecision.Stop(
                            FenceStop(FenceStopCause.NAME_PROBE_FAILED, "the table named like folder=$folderPath could not be looked up", th),
                        )
                    }
                }
            when (namedTable) {
                NamedTableStorage.NotFound -> Unit
                // Such a table may use files outside its declared paths, as at the full read, so no folder is proven unowned.
                is NamedTableStorage.Unresolved ->
                    return FenceDecision.Stop(
                        FenceStop(
                            FenceStopCause.NAME_PROBE_FAILED,
                            "the storage of the table named like folder=$folderPath could not be established",
                            IllegalStateException(namedTable.reason),
                        ),
                    )
                is NamedTableStorage.Found -> {
                    val paths = listOfNotNull(namedTable.location) + namedTable.writeLocations
                    if (paths.any { runCatching { !StoragePathUtils.isQualifiedLocation(it) }.getOrDefault(true) }) {
                        return FenceDecision.Stop(
                            FenceStop(FenceStopCause.NAME_PROBE_FAILED, "the table named like folder=$folderPath has a location with no scheme and bucket"),
                        )
                    }
                    val tablePaths = paths.map(StoragePathUtils::normalizeLocation)
                    protectedPaths += tablePaths
                    // A table recreated elsewhere under the same name does not claim its old folder.
                    tablePaths.firstOrNull { StoragePathUtils.overlaps(folder, it) }?.let { tablePath ->
                        logger.warn(
                            "Skipping deletion because a table of catalog=$catalog, database=$database named like the folder stores " +
                                "files in it: path=$folderPath, tablePath=$tablePath"
                        )
                        return FenceDecision.Keep(FenceKeepReason.TABLE_NAMED_LIKE_FOLDER)
                    }
                }
            }

            val owner = protectedPaths.firstOrNull { StoragePathUtils.overlaps(folder, it) } ?: return FenceDecision.Proceed
            logger.warn("Skipping deletion because the folder overlaps protected storage: path=$folderPath, protectedPath=$owner")
            return FenceDecision.Keep(FenceKeepReason.CLAIMED)
        }

        override fun checkListing(listing: FolderListing.Listed): FenceDecision {
            val inspected = inspectionFingerprints[StoragePathUtils.normalizeLocation(listing.folderPath)]
            if (inspected?.value != listing.fingerprint.value) {
                logger.warn(
                    "Skipping deletion because the folder changed since inspection: path=${listing.folderPath}, " +
                        "inspected=${inspected?.value}, listed=${listing.fingerprint.value}"
                )
                return FenceDecision.Keep(FenceKeepReason.CHANGED_SINCE_INSPECTION)
            }

            val newest = listing.fingerprint.newestModificationTimeMillis
            if (newest == null || listing.objects.any { it.modificationTimeMillis <= 0 }) {
                return FenceDecision.Keep(FenceKeepReason.UNKNOWN_AGE)
            }
            if (newest > fence.cutoffTimeMillis) {
                logger.warn("Skipping deletion because an object is newer than the age cutoff: path=${listing.folderPath}, newest=$newest")
                return FenceDecision.Keep(FenceKeepReason.TOO_NEW)
            }
            return FenceDecision.Proceed
        }

        override fun stopBeforeNextObject(): FenceStop? =
            if (snapshotAgeNanos() > maxAgeNanos) {
                FenceStop(
                    FenceStopCause.SNAPSHOT_EXPIRED,
                    "the ownership snapshot became older than ${limitText()} during deletion",
                )
            } else {
                null
            }

        override fun stopBeforeDelete(): FenceStop? =
            stopBeforeNextObject()
                ?: if (snapshotAgeNanos() > maxAgeNanos - startMargin.toNanos()) {
                    FenceStop(
                        FenceStopCause.SNAPSHOT_EXPIRED,
                        "the ownership snapshot was within ${text(startMargin)} of its ${limitText()} limit during deletion, so no further delete was started",
                    )
                } else {
                    null
                }

        private fun snapshotDeadline(): Long = snapshotStartedAt?.let { it + maxAgeNanos } ?: ticker.nanos()

        override fun readWait(): Duration = Duration.ofNanos(snapshotDeadline() - ticker.nanos())

        // A delete still running when the snapshot reaches its limit is not waited for, so a delete counted as done
        // returned before then. One that did not may still land later.
        override fun deleteWait(): Duration = Duration.ofNanos(snapshotDeadline() - ticker.nanos())

        override fun readTimedOut(error: CallTimedOutException): FenceStop =
            FenceStop(
                FenceStopCause.READ_TIMED_OUT,
                if (error.started) {
                    "the job stopped waiting to ${error.operation} for ${error.target} when the ownership snapshot reached the ${limitText()} limit"
                } else {
                    "the job did not ${error.operation} for ${error.target}, because the ownership snapshot had reached the ${limitText()} limit"
                },
                error,
            )
    }
}
