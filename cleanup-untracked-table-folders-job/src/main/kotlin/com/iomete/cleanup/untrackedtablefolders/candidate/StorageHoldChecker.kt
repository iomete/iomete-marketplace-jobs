package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistory
import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistoryUnavailableException
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.FolderMetadataFile
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.IcebergUuidVeto
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.LiveIcebergObject
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.NameMappedManifestReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.StaticIcebergTables
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason
import com.iomete.cleanup.untrackedtablefolders.audit.HISTORY_LOOKBACK
import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateFolderStatistics
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogFileSystemProvider
import com.iomete.cleanup.untrackedtablefolders.storage.DataOnlyShape
import com.iomete.cleanup.untrackedtablefolders.storage.FolderLayout
import com.iomete.cleanup.untrackedtablefolders.storage.MetadataVerdict
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopFileIO

/** Why a folder that passed every ownership and age check is still kept. The code names the reason in audit rows. */
enum class StorageHoldReason(val code: String) {
    INSPECTION_INCOMPLETE("inspection_incomplete"),
    METADATA_VIEW(MetadataVerdict.VIEW.code),
    METADATA_UNREADABLE(MetadataVerdict.UNREADABLE.code),
    METADATA_NO_UUID(MetadataVerdict.NO_UUID.code),
    METADATA_LOCATION_MISMATCH(MetadataVerdict.LOCATION_MISMATCH.code),
    DATA_ONLY_UNKNOWN_PROVENANCE("data_only_unknown_provenance"),
    DATA_ONLY_NOT_PLAIN_OUTPUT("data_only_not_plain_output"),
    LIVE_TABLES_INCOMPLETE("live_tables_incomplete"),
    LIVE_TABLE_UUID("live_table_uuid"),
    LIVE_FILE_REFERENCED("live_file_referenced"),
    NAME_MAPPED_FILE_REFERENCED("name_mapped_file_referenced"),
    NAME_MAPPED_READ_INCOMPLETE("name_mapped_read_incomplete"),
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

/** A live Iceberg table or view and the catalog it was read from. */
data class LiveIcebergTable(
    val catalog: String,
    val table: LiveIcebergObject,
)

/** The live Iceberg tables and views read for a database's cleanup roots. Incomplete when any of those reads was. */
data class LiveIcebergTables(
    val tables: List<LiveIcebergTable>,
    val complete: Boolean,
)

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
 * table, data that is not only commit scratch, metadata or files a live table still uses, or a folder not yet seen
 * unchanged by earlier runs of the same job for the confirmation window. A folder gets the first reason that applies,
 * in that order.
 */
@ApplicationScoped
class StorageHoldChecker {
    private val logger = SafeLogger.getLogger(StorageHoldChecker::class.java)

    @Inject lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService

    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService

    @Inject lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    // Tests set this to load tables from in-memory files.
    internal var tableLoader: ((LiveIcebergTable) -> Table)? = null

    private val nameMappedManifestReader = NameMappedManifestReader()

    fun check(
        catalog: String,
        database: String,
        eligibleFolders: List<String>,
        statistics: CandidateFolderStatistics,
        history: AuditHistory,
        listingTime: Instant,
        live: LiveIcebergTables,
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
        val metadata = objectStorageDiscoveryService.inspectMetadata(catalog, metadataKeys)
        metadataKeys.keys.forEach { path ->
            val reason = StorageHoldReason.of(metadata[path]?.verdict ?: MetadataVerdict.UNREADABLE)
            if (reason == null) remaining += path else hold(reason, path)
        }

        val unused = liveTableHolds(remaining.sorted(), metadata.mapValues { it.value.tableUuids }, live, ::hold)

        unused.forEach { path ->
            confirmationHold(path, statistics, history, listingTime)?.let { hold(it, path) }
        }

        return StorageHolds(held.toSortedMap().mapValues { (_, paths) -> paths.sorted() }, history)
    }

    // The UUID veto, then a manifest read of every name-mapped table, which can point at files in any folder.
    private fun liveTableHolds(
        folders: List<String>,
        tableUuids: Map<String, Set<String>>,
        live: LiveIcebergTables,
        hold: (StorageHoldReason, String) -> Unit,
    ): List<String> {
        if (folders.isEmpty()) {
            return folders
        }
        val veto = IcebergUuidVeto(live.tables.map { it.table }, live.complete)
        val unclaimed =
            folders.filter { folder ->
                val files = tableUuids[folder].orEmpty().map { FolderMetadataFile(folder, readable = true, uuid = it, isView = false, location = folder) }
                val reason = runCatching { veto.keepReasons(folder, files).firstOrNull() }.getOrDefault(UuidKeepReason.LIVE_SET_INCOMPLETE)
                reason?.let { hold(holdReason(it), folder) }
                reason == null
            }

        val nameMapped = live.tables.filter { it.table.nameMapped && !it.table.isView }
        if (unclaimed.isEmpty() || nameMapped.isEmpty()) {
            return unclaimed
        }
        val referenced = sortedSetOf<String>()
        for (table in nameMapped) {
            val outcome =
                try {
                    nameMappedManifestReader.referencedFiles(table.table.name, load(table), unclaimed)
                } catch (th: Exception) {
                    logger.warn("Could not read the manifests of a name-mapped table of catalog=${table.catalog}", th)
                    null
                }
            when (outcome) {
                is NameMappedManifestReader.Outcome.Read ->
                    outcome.filesInFolders.forEach { file -> referenced += unclaimed.filter { folder -> mayHold(folder, file) } }
                NameMappedManifestReader.Outcome.NotNameMapped -> Unit
                is NameMappedManifestReader.Outcome.Unknown, null -> {
                    logger.warn(
                        "Holding ${unclaimed.size} folder(s): a name-mapped table of catalog=${table.catalog} could not be read " +
                            "for the files it uses (${(outcome as? NameMappedManifestReader.Outcome.Unknown)?.gap?.reason ?: "load_failed"})"
                    )
                    unclaimed.forEach { hold(StorageHoldReason.NAME_MAPPED_READ_INCOMPLETE, it) }
                    return emptyList()
                }
            }
        }
        referenced.forEach { hold(StorageHoldReason.NAME_MAPPED_FILE_REFERENCED, it) }
        return unclaimed - referenced
    }

    private fun load(table: LiveIcebergTable): Table {
        tableLoader?.let { return it(table) }
        val metadataFile = checkNotNull(table.table.metadataFileLocation) { "the table has no metadata file location" }
        return StaticIcebergTables.load(table.table.name, metadataFile, HadoopFileIO(catalogFileSystemProvider.catalogReadConfiguration(table.catalog)))
    }

    // A file without scheme and bucket is placed in the folder's bucket; one that can't be placed may be in any folder.
    private fun mayHold(
        folder: String,
        file: String,
    ): Boolean =
        runCatching {
            val placed = StoragePathUtils.inBucketOf(file, folder) ?: return true
            StoragePathUtils.isSameOrChildLocation(placed, folder)
        }.getOrDefault(true)

    private fun holdReason(reason: UuidKeepReason): StorageHoldReason =
        when (reason) {
            UuidKeepReason.LIVE_SET_INCOMPLETE -> StorageHoldReason.LIVE_TABLES_INCOMPLETE
            UuidKeepReason.LIVE_UUID -> StorageHoldReason.LIVE_TABLE_UUID
            UuidKeepReason.LIVE_FILE_REFERENCED -> StorageHoldReason.LIVE_FILE_REFERENCED
            UuidKeepReason.METADATA_UNREADABLE -> StorageHoldReason.METADATA_UNREADABLE
            UuidKeepReason.UUID_MISSING -> StorageHoldReason.METADATA_NO_UUID
            UuidKeepReason.VIEW_METADATA -> StorageHoldReason.METADATA_VIEW
            UuidKeepReason.LOCATION_ELSEWHERE -> StorageHoldReason.METADATA_LOCATION_MISMATCH
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
