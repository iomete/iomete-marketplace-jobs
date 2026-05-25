package com.iomete.cleanup.untrackedtablefolders.storage

import java.net.URI
import org.apache.hadoop.fs.Path

object StoragePathUtils {
    fun normalizeLocation(location: String): String {
        val trimmed = location.trim().trimEnd('/')

        require(trimmed.isNotBlank()) {
            "storage location must not be blank"
        }

        val uri = Path(trimmed).toUri().normalize()

        val scheme = uri.scheme?.lowercase()?.let(::canonicalizeScheme)
        val authority = uri.authority?.lowercase()
        val normalizedPath = normalizeUriPath(uri)

        return when {
            scheme != null && authority != null -> "$scheme://$authority$normalizedPath"
            scheme != null -> "$scheme:$normalizedPath"
            else -> normalizedPath
        }.trimEnd('/')
    }

    fun isSameOrChildLocation(
        candidateLocation: String,
        rootLocation: String,
    ): Boolean {
        val candidate = normalizeLocation(candidateLocation)
        val root = normalizeLocation(rootLocation)

        return candidate == root || candidate.startsWith("$root/")
    }

    fun allowedDatabaseRoots(databaseLocation: String): Set<String> {
        val normalizedDatabaseLocation = normalizeLocation(databaseLocation)

        return buildSet {
            add(normalizedDatabaseLocation)

            if (normalizedDatabaseLocation.endsWith(".db")) {
                add(normalizedDatabaseLocation.removeSuffix(".db"))
            }
        }
    }

    fun isInsideAnyRoot(
        candidateLocation: String,
        rootLocations: Set<String>,
    ): Boolean =
        rootLocations.any { rootLocation ->
            isSameOrChildLocation(
                candidateLocation = candidateLocation,
                rootLocation = rootLocation,
            )
        }

    private fun canonicalizeScheme(scheme: String): String =
        when (scheme) {
            "s3", "s3a", "s3n" -> "s3a"
            else -> scheme
        }

    private fun normalizeUriPath(uri: URI): String {
        val path = uri.path
            ?.replace(Regex("/{2,}"), "/")
            ?.ifBlank { "/" }
            ?: "/"

        return if (path.startsWith("/")) path else "/$path"
    }
}
