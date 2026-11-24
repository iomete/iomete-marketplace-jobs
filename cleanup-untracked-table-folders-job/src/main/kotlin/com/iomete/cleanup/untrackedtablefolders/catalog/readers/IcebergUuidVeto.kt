package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils

/** One *.metadata.json found in a candidate folder, as the storage side read its head. */
data class FolderMetadataFile(
    val path: String,
    val readable: Boolean,
    val uuid: String?,
    val isView: Boolean,
    val location: String?,
)

enum class UuidKeepReason {
    LIVE_SET_INCOMPLETE,
    METADATA_UNREADABLE,
    UUID_MISSING,
    VIEW_METADATA,
    LIVE_UUID,
    LOCATION_ELSEWHERE,
    LIVE_FILE_REFERENCED,
}

/**
 * Table UUID veto. Keeps a folder when its metadata could belong to a live table or view.
 *
 * It only ever keeps. An empty result is not permission to delete: a folder still has to pass every other
 * check, and a missing UUID never overrides one of them.
 */
class IcebergUuidVeto(live: List<LiveIcebergObject>, private val liveSetComplete: Boolean) {
    private val liveUuids = live.mapNotNull { it.uuid?.trim()?.lowercase() }.toSet()
    private val liveFiles = live.flatMap { listOfNotNull(it.metadataFileLocation) + it.referencedFiles }
        .filter { it.isNotBlank() }
        .map { runCatching { StoragePathUtils.normalizeLocation(it) }.getOrNull() }
    private val allLiveFilesComparable = liveFiles.none { it == null }

    fun keepReasons(folder: String, metadataFiles: List<FolderMetadataFile>): Set<UuidKeepReason> {
        val reasons = sortedSetOf<UuidKeepReason>()
        if (!liveSetComplete || !allLiveFilesComparable) reasons += UuidKeepReason.LIVE_SET_INCOMPLETE
        val normalizedFolder = StoragePathUtils.normalizeLocation(folder)
        metadataFiles.forEach { file ->
            val uuid = file.uuid?.trim()?.lowercase()
            when {
                !file.readable -> reasons += UuidKeepReason.METADATA_UNREADABLE
                uuid.isNullOrEmpty() -> reasons += UuidKeepReason.UUID_MISSING
                file.isView -> reasons += UuidKeepReason.VIEW_METADATA
                uuid in liveUuids -> reasons += UuidKeepReason.LIVE_UUID
            }
            if (file.readable && !sameLocation(file.location, normalizedFolder)) reasons += UuidKeepReason.LOCATION_ELSEWHERE
        }
        if (liveFiles.any { it != null && (it == normalizedFolder || it.startsWith("$normalizedFolder/")) }) {
            reasons += UuidKeepReason.LIVE_FILE_REFERENCED
        }
        return reasons
    }

    private fun sameLocation(location: String?, normalizedFolder: String): Boolean =
        location != null && runCatching { StoragePathUtils.normalizeLocation(location) == normalizedFolder }.getOrDefault(false)
}
