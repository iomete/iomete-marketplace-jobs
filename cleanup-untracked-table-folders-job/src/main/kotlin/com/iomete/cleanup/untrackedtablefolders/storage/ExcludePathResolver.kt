package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject

@ApplicationScoped
class ExcludePathResolver {

    @Inject lateinit var config: ApplicationConfig

    fun normalizedConfiguredExcludePaths(): List<String> =
        config.excludePaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .distinct()
            .sorted()

    fun effectiveExcludedPaths(
        database: String,
        storageScanLocation: String,
    ): List<String> {
        val configuredExcludePaths = normalizedConfiguredExcludePaths()
        val databaseFolderExcludePaths =
            resolvedExcludeDatabaseFolderPaths(
                database = database,
                storageScanLocation = storageScanLocation,
            )

        return (configuredExcludePaths + databaseFolderExcludePaths)
            .map { StoragePathUtils.normalizeLocation(it) }
            .distinct()
            .sorted()
    }

    private fun resolvedExcludeDatabaseFolderPaths(
        database: String,
        storageScanLocation: String,
    ): List<String> =
        config.excludeDatabaseFolders.mapNotNull { excludedDatabaseFolder ->
            val parts = excludedDatabaseFolder.split(".", limit = 2)
            if (parts.size != 2) return@mapNotNull null

            val excludedDatabase = parts[0]
            val excludedFolder = parts[1]

            if (excludedDatabase != database) return@mapNotNull null
            StoragePathUtils.normalizeLocation("$storageScanLocation/$excludedFolder")
        }
}
