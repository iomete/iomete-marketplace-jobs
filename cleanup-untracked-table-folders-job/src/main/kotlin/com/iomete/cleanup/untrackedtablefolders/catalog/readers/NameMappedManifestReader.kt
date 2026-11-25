package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import org.apache.iceberg.ManifestContent
import org.apache.iceberg.ManifestFile
import org.apache.iceberg.ManifestFiles
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties

/**
 * add_files, migrate, snapshot and Parquet table registration point a table at files in other folders, and
 * all of them set a name mapping. Only those tables get a manifest read, and only files inside [folders] are
 * returned.
 */
class NameMappedManifestReader(private val limits: ReaderLimits = ReaderLimits()) {
    sealed interface Outcome {
        data object NotNameMapped : Outcome

        data class Read(val filesInFolders: List<String>) : Outcome

        data class Unknown(val gap: ReadGap) : Outcome
    }

    fun referencedFiles(name: List<String>, table: Table, folders: List<String>): Outcome {
        if (!table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) return Outcome.NotNameMapped
        return try {
            read(name, table, folders.map(StoragePathUtils::normalizeLocation))
        } catch (e: Exception) {
            Outcome.Unknown(gapFor(name, "manifest_read_failed", e))
        }
    }

    private fun read(name: List<String>, table: Table, folders: List<String>): Outcome {
        val io = table.io()
        // Every snapshot, so branches, tags and older snapshots still in the table count too.
        val manifests = linkedMapOf<String, ManifestFile>()
        table.snapshots().forEach { snapshot ->
            snapshot.allManifests(io).forEach { manifests.putIfAbsent(it.path(), it) }
            if (manifests.size > limits.maxManifestsPerTable) {
                return Outcome.Unknown(ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "too_many_manifests"))
            }
        }
        val found = linkedSetOf<String>()
        var entries = 0L
        fun visit(path: String): Boolean {
            entries++
            if (folders.any { isInside(path, it) }) found += path
            return entries <= limits.maxManifestEntriesPerTable
        }
        manifests.values.forEach { manifest ->
            // readPaths refuses delete manifests, so delete files need their own reader.
            val withinLimit = when (manifest.content()) {
                ManifestContent.DATA -> ManifestFiles.readPaths(manifest, io).use { paths -> paths.all { visit(it) } }
                ManifestContent.DELETES ->
                    ManifestFiles.readDeleteManifest(manifest, io, table.specs()).use { files -> files.all { visit(it.location()) } }
                else -> return Outcome.Unknown(ReadGap(name, ReadGapKind.UNSUPPORTED, "unknown_manifest_content"))
            }
            if (!withinLimit) return Outcome.Unknown(ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "too_many_manifest_entries"))
        }
        return Outcome.Read(found.toList())
    }

    // A path without scheme and bucket is compared in the folder's bucket. One that can't be placed or parsed
    // can't be shown to be outside the folder, so it counts as inside.
    private fun isInside(path: String, folder: String): Boolean =
        runCatching {
            val placed = StoragePathUtils.inBucketOf(path, folder) ?: return true
            StoragePathUtils.isSameOrChildLocation(placed, folder)
        }.getOrDefault(true)
}
