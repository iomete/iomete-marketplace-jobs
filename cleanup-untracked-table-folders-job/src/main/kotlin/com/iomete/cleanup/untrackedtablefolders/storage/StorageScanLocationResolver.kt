package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

@ApplicationScoped
class StorageScanLocationResolver {

    private val logger = Logger.getLogger(StorageScanLocationResolver::class.java)

    fun resolve(
        databaseLocation: String,
        activeTableLocations: List<String>,
    ): String {
        val inferredScanLocations =
            activeTableLocations.mapNotNull { parentLocation(it) }.distinct().sorted()

        val resolvedScanLocation =
            when (inferredScanLocations.size) {
                0 -> {
                    logger.warn(
                        "No active table locations were discovered for databaseLocation=$databaseLocation. Falling back to discovered database location for storage scan. This may miss untracked folders if the database location differs from the actual table storage root."
                    )
                    databaseLocation
                }
                1 -> inferredScanLocations.single()
                else -> {
                    logger.warn(
                        "Multiple active table parent locations were discovered for databaseLocation=$databaseLocation: $inferredScanLocations. Falling back to database location."
                    )
                    databaseLocation
                }
            }

        validateStorageScanLocation(
            databaseLocation = databaseLocation,
            storageScanLocation = resolvedScanLocation,
        )

        return resolvedScanLocation
    }

    private fun validateStorageScanLocation(
        databaseLocation: String,
        storageScanLocation: String,
    ) {
        val allowedRoots = StoragePathUtils.allowedDatabaseRoots(databaseLocation)

        if (!StoragePathUtils.isInsideAnyRoot(storageScanLocation, allowedRoots)) {
            throw IllegalStateException(
                "Resolved storage scan location escapes database boundary. databaseLocation=$databaseLocation, storageScanLocation=$storageScanLocation, allowedRoots=$allowedRoots"
            )
        }
    }

    private fun parentLocation(location: String): String? {
        val normalizedLocation = location.trim().trimEnd('/')
        val lastSlashIndex = normalizedLocation.lastIndexOf('/')

        return if (lastSlashIndex <= 0) {
            null
        } else {
            normalizedLocation.substring(0, lastSlashIndex)
        }
    }
}