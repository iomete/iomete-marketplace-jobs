package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils

/**
 * Only COMPLETE means every object of the catalog was read. Any other status leaves owners unknown, and an
 * unknown owner must never make a folder unowned.
 *
 * COMPLETE is not proof that a folder is safe: an [ExternalPathUse] with an unknownReason is storage the reader
 * could not bound, and still needs conservative protection.
 */
enum class CatalogReadStatus { COMPLETE, PARTIAL, FAILED, UNAUTHORIZED }

enum class ReadGapKind { FAILED, UNAUTHORIZED, LIMIT_EXCEEDED, UNVERIFIED, UNSUPPORTED }

/**
 * Something the reader could not read. [name] is the object path in the catalog, empty for the catalog itself.
 * [location] is set for a location without scheme and bucket that still has an absolute path.
 */
data class ReadGap(
    val name: List<String>,
    val kind: ReadGapKind,
    val reason: String,
    val causeClass: String? = null,
    val location: String? = null,
)

enum class LocationRole { NAMESPACE, TABLE, PARTITION, WRITE_PATH, VIEW }

/** [storesFiles] is false for a JDBC table's default location: the rows live in another database. */
data class OwnedLocation(
    val name: List<String>,
    val role: LocationRole,
    val location: String,
    val storesFiles: Boolean = true,
)

/** An Iceberg table or view the catalog says is live. [referencedFiles] are paths named in its current metadata. */
data class LiveIcebergObject(
    val name: List<String>,
    val uuid: String?,
    val isView: Boolean,
    val location: String?,
    val metadataFileLocation: String?,
    val referencedFiles: List<String>,
    val nameMapped: Boolean,
)

/**
 * Storage an object uses outside its own folder: paths a view reads, or a reason the paths can't be listed
 * (symlink manifest tables, custom location providers, view text that can't be parsed).
 */
data class ExternalPathUse(
    val name: List<String>,
    val paths: List<String>,
    val unknownReason: String? = null,
)

data class CatalogReadResult(
    val catalog: String,
    val locations: List<OwnedLocation>,
    val liveIceberg: List<LiveIcebergObject>,
    val externalPathUse: List<ExternalPathUse>,
    val gaps: List<ReadGap>,
    val namespaces: List<List<String>> = emptyList(),
) {
    val status: CatalogReadStatus
        get() {
            val catalogGaps = gaps.filter { it.name.isEmpty() }
            return when {
                catalogGaps.any { it.kind == ReadGapKind.UNAUTHORIZED } -> CatalogReadStatus.UNAUTHORIZED
                catalogGaps.any { it.kind == ReadGapKind.FAILED || it.kind == ReadGapKind.UNSUPPORTED } ->
                    CatalogReadStatus.FAILED
                gaps.isNotEmpty() -> CatalogReadStatus.PARTIAL
                else -> CatalogReadStatus.COMPLETE
            }
        }

    companion object {
        fun failed(catalog: String, gap: ReadGap) = CatalogReadResult(catalog, emptyList(), emptyList(), emptyList(), listOf(gap))
    }
}

data class ReaderLimits(
    val maxNamespaces: Int = 10_000,
    val maxNamespaceDepth: Int = 16,
    val tablesByNameBatch: Int = 100,
    val maxPartitionsPerTable: Int = 50_000,
    val maxManifestsPerTable: Int = 10_000,
    val maxManifestEntriesPerTable: Long = 5_000_000,
)

internal class ReadAccumulator(private val catalog: String) {
    val locations = mutableListOf<OwnedLocation>()
    val liveIceberg = mutableListOf<LiveIcebergObject>()
    val externalPathUse = mutableListOf<ExternalPathUse>()
    val gaps = mutableListOf<ReadGap>()
    val namespaces = mutableListOf<List<String>>()

    // A path without scheme and bucket resolves against the writer's filesystem and can't be compared.
    fun location(name: List<String>, role: LocationRole, location: String?, storesFiles: Boolean = true) {
        if (location.isNullOrBlank()) return
        if (runCatching { StoragePathUtils.isQualifiedLocation(location) }.getOrDefault(false)) {
            locations += OwnedLocation(name, role, location, storesFiles)
        } else {
            val placeable = location.takeIf { StoragePathUtils.unqualifiedPath(it) != null }
            gaps += ReadGap(name, ReadGapKind.UNVERIFIED, "${role.name.lowercase()}_location_not_qualified", location = placeable)
        }
    }

    fun gap(name: List<String>, reason: String, error: Exception) {
        gaps += gapFor(name, reason, error)
    }

    fun result() =
        CatalogReadResult(catalog, locations.toList(), liveIceberg.toList(), externalPathUse.toList(), gaps.toList(), namespaces.toList())
}

private val UNAUTHORIZED_ERRORS = setOf(
    "org.apache.iceberg.exceptions.NotAuthorizedException",
    "org.apache.iceberg.exceptions.ForbiddenException",
    "org.apache.hadoop.security.AccessControlException",
    "java.nio.file.AccessDeniedException",
    "java.lang.SecurityException",
)

// Only class names are kept. Messages can carry server text, paths or credentials.
internal fun gapFor(name: List<String>, reason: String, error: Exception): ReadGap {
    val unauthorized = generateSequence<Throwable>(error) { it.cause }.take(10).any { it.javaClass.name in UNAUTHORIZED_ERRORS }
    val kind = if (unauthorized) ReadGapKind.UNAUTHORIZED else ReadGapKind.FAILED
    return ReadGap(name, kind, reason, error.javaClass.simpleName)
}
