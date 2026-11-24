package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.FileNotFoundException

data class StorageFolder(
    val path: String,
)

data class StorageSizeStats(
    val objectCount: Long,
    val totalSizeBytes: Long,
    val newestObjectModificationTimeMillis: Long? = null,
) {
    companion object {
        val ZERO = StorageSizeStats(objectCount = 0, totalSizeBytes = 0)
    }
}

data class SizeStatsBatch(
    val statsByFolder: Map<String, StorageSizeStats>,
    val failures: Map<String, Throwable>,
    val absentFolderPaths: Set<String> = emptySet(),
    val tooManyObjectsFolderPaths: Set<String> = emptySet(),
    val fingerprintsByFolder: Map<String, FolderFingerprint> = emptyMap(),
    val layoutsByFolder: Map<String, FolderLayout> = emptyMap(),
)

// Exact-key deletion sends about four requests per object, one object at a time, and needs the whole key list in
// memory, so a folder above this many objects is held, at inspection and again before deletion.
const val MAX_OBJECTS_PER_FOLDER = 10_000

sealed interface FolderListing {
    val folderPath: String

    data class Listed(
        override val folderPath: String,
        val objects: List<ListedObject>,
    ) : FolderListing {
        val fingerprint: FolderFingerprint = FolderFingerprint.of(objects)
    }

    /** The folder holds more than [maxObjects] objects. It is never treated as empty, unowned or deletable. */
    data class TooManyObjects(
        override val folderPath: String,
        val maxObjects: Int,
    ) : FolderListing
}

object FolderObjectListing {
    /**
     * Lists every object below [folderPath], stopping at the first object past [maxObjects]. A missing folder throws
     * the filesystem's FileNotFoundException. An object outside the folder, a directory entry or a key that does not
     * map back to its own path fails the listing, so no caller can act on it.
     */
    fun list(
        fileSystem: FileSystem,
        folderPath: String,
        maxObjects: Int = MAX_OBJECTS_PER_FOLDER,
    ): FolderListing {
        require(maxObjects >= 0) { "maxObjects must be greater than or equal to 0" }

        val folder = folderPrefix(folderPath)
        val files = fileSystem.listFiles(Path(folderPath), true)
        val objects = ArrayList<ListedObject>()

        while (files.hasNext()) {
            val status = files.next()
            if (objects.size == maxObjects) {
                return FolderListing.TooManyObjects(folderPath, maxObjects)
            }
            check(!status.isDirectory) { "Listing of $folder returned a directory entry: ${status.path}" }
            objects += ListedObject(relativeKey(folder, status.path.toString()), status.len, status.modificationTime, etagOf(status))
        }

        return FolderListing.Listed(folderPath, objects)
    }

    fun folderPrefix(folderPath: String): String = Path(folderPath).toString().trimEnd('/')

    // Hadoop's Path collapses "//", drops a trailing "/" and resolves "." and "..", so a key is used only if its path
    // maps back to itself.
    fun objectPath(
        folder: String,
        relativeKey: String,
    ): Path {
        val path = Path("$folder/$relativeKey")
        check(path.toString() == "$folder/$relativeKey") { "Object key is not a plain path below $folder: $relativeKey" }
        return path
    }

    private fun relativeKey(
        folder: String,
        objectPath: String,
    ): String {
        check(objectPath.startsWith("$folder/")) { "Listing of $folder returned an object outside it: $objectPath" }
        return objectPath.substring(folder.length + 1).also { objectPath(folder, it) }
    }
}

@ApplicationScoped
class ObjectStorageDiscoveryService {
    private val logger = SafeLogger.getLogger(ObjectStorageDiscoveryService::class.java)

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    fun listImmediateChildFolders(
        catalog: String,
        location: String,
    ): List<StorageFolder> {
        logger.info("Listing immediate child folders for catalog=$catalog under location=$location")

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "list immediate child folders",
            locations = listOf(location),
        ) { fileSystem ->
            childFolders(fileSystem, Path(location))
        }
    }

    fun listImmediateChildFoldersIfPresent(
        catalog: String,
        location: String,
    ): List<StorageFolder>? {
        logger.info("Listing immediate child folders for catalog=$catalog under location=$location")

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "list immediate child folders",
            locations = listOf(location),
        ) { fileSystem ->
            val path = Path(location)
            if (fileSystem.exists(path)) childFolders(fileSystem, path) else null
        }
    }

    private fun childFolders(
        fileSystem: FileSystem,
        path: Path,
    ): List<StorageFolder> =
        fileSystem
            .listStatus(path)
            .filter { it.isDirectory }
            .map { StorageFolder(path = it.path.toString()) }
            .sortedBy { it.path }

    fun collectSizeStatsPerFolder(
        catalog: String,
        folderPaths: List<String>,
    ): SizeStatsBatch {
        if (folderPaths.isEmpty()) {
            return SizeStatsBatch(emptyMap(), emptyMap())
        }

        val sortedFolderPaths = folderPaths.sorted()

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "collect size statistics",
            locations = sortedFolderPaths,
        ) { fileSystem ->
            val statsByFolder = linkedMapOf<String, StorageSizeStats>()
            val failures = linkedMapOf<String, Throwable>()
            val absentFolderPaths = linkedSetOf<String>()
            val tooManyObjectsFolderPaths = linkedSetOf<String>()
            val fingerprintsByFolder = linkedMapOf<String, FolderFingerprint>()
            val layoutsByFolder = linkedMapOf<String, FolderLayout>()

            sortedFolderPaths.forEach { folderPath ->
                logger.info("Collecting size statistics for candidate folder: $folderPath")

                try {
                    val listing =
                        catalogFileSystemProvider.runOperation(catalog, "collect size statistics", folderPath) {
                            FolderObjectListing.list(fileSystem, folderPath)
                        }

                    when (listing) {
                        is FolderListing.TooManyObjects -> {
                            logger.warn(
                                "Candidate folder holds more than ${listing.maxObjects} objects and is kept as too_many_objects: path=$folderPath"
                            )
                            tooManyObjectsFolderPaths += folderPath
                        }

                        is FolderListing.Listed -> {
                            val folderStats = sizeOf(listing)
                            logger.info(
                                "Collected size statistics for candidate folder=$folderPath: objectCount=${folderStats.objectCount}, totalSizeBytes=${folderStats.totalSizeBytes}"
                            )
                            statsByFolder[folderPath] = folderStats
                            fingerprintsByFolder[folderPath] = listing.fingerprint
                            layoutsByFolder[folderPath] = FolderLayout.of(listing.objects)
                        }
                    }
                } catch (th: CatalogStorageOperationException) {
                    val failure = if (th.cause is FileNotFoundException) recheckNotFound(catalog, fileSystem, folderPath, th) else th

                    if (failure == null) {
                        logger.info("Candidate folder was listed but no longer exists: path=$folderPath")
                        absentFolderPaths += folderPath
                    } else {
                        failures[folderPath] = failure
                    }
                }
            }

            SizeStatsBatch(statsByFolder, failures, absentFolderPaths, tooManyObjectsFolderPaths, fingerprintsByFolder, layoutsByFolder)
        }
    }

    /**
     * Reads the head of every metadata file of each folder, in key order, and stops a folder at its first file that
     * holds it. A file that cannot be opened or parsed holds its folder as [MetadataVerdict.UNREADABLE].
     */
    fun inspectMetadata(
        catalog: String,
        metadataKeysByFolder: Map<String, List<String>>,
    ): Map<String, FolderMetadata> {
        if (metadataKeysByFolder.isEmpty()) {
            return emptyMap()
        }
        val verdicts = linkedMapOf<String, FolderMetadata>()
        metadataKeysByFolder.keys.sorted().groupBy { Path(it).toUri().let { uri -> "${uri.scheme}://${uri.authority}" } }.values.forEach { folders ->
            try {
                catalogFileSystemProvider.withFileSystem(catalog, "read table metadata", folders) { fileSystem ->
                    folders.forEach { folder -> verdicts[folder] = folderVerdict(catalog, fileSystem, folder, metadataKeysByFolder.getValue(folder)) }
                }
            } catch (th: IllegalStateException) {
                if (th !is CatalogStorageOperationException && th !is CatalogStorageConfigurationException) {
                    throw th
                }
                logger.warn("Could not read table metadata of ${folders.size} candidate folder(s); they are kept: paths=$folders", th)
                folders.forEach { verdicts.putIfAbsent(it, FolderMetadata(MetadataVerdict.UNREADABLE)) }
            }
        }
        return verdicts
    }

    private fun folderVerdict(
        catalog: String,
        fileSystem: FileSystem,
        folderPath: String,
        metadataKeys: List<String>,
    ): FolderMetadata {
        if (metadataKeys.isEmpty()) {
            logger.warn("Candidate folder has a table version hint but no metadata file to read; it is kept: path=$folderPath")
            return FolderMetadata(MetadataVerdict.UNREADABLE)
        }
        val folder = FolderObjectListing.folderPrefix(folderPath)
        val tableUuids = sortedSetOf<String>()
        for (key in metadataKeys) {
            val metadata =
                try {
                    catalogFileSystemProvider.runOperation(catalog, "read table metadata", "$folder/$key") {
                        fileSystem.open(FolderObjectListing.objectPath(folder, key)).use { input ->
                            MetadataHead.read(input, FolderLayout.isGzipMetadataFile(key.substringAfterLast('/')), folderPath)
                        }
                    }
                } catch (th: CatalogStorageOperationException) {
                    logger.warn("Could not read table metadata file of candidate folder; it is kept: path=$folderPath, key=$key", th)
                    FolderMetadata(MetadataVerdict.UNREADABLE)
                }
            if (metadata.verdict != MetadataVerdict.OWN_TABLE) {
                logger.warn("Keeping candidate folder because of its table metadata: path=$folderPath, key=$key, reason=${metadata.verdict.code}")
                return metadata
            }
            tableUuids += metadata.tableUuids
        }
        return FolderMetadata(MetadataVerdict.OWN_TABLE, tableUuids)
    }

    // Returns null only when a second lookup also finds nothing; any other answer keeps the folder a failure.
    private fun recheckNotFound(
        catalog: String,
        fileSystem: FileSystem,
        folderPath: String,
        notFound: CatalogStorageOperationException,
    ): Throwable? =
        try {
            catalogFileSystemProvider.runOperation(catalog, "recheck candidate folder", folderPath) {
                fileSystem.getFileStatus(Path(folderPath))
            }
            IllegalStateException(
                "Candidate folder was not found while collecting statistics but exists on recheck: location=$folderPath",
                notFound,
            )
        } catch (recheck: CatalogStorageOperationException) {
            if (recheck.cause is FileNotFoundException) null else recheck
        }

    fun collectSizeStats(
        catalog: String,
        folderPaths: List<String>,
    ): StorageSizeStats {
        val batch = collectSizeStatsPerFolder(catalog, folderPaths)

        batch.failures.values.firstOrNull()?.let { throw it }

        return batch.statsByFolder.values.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }
    }

    private fun sizeOf(listing: FolderListing.Listed): StorageSizeStats {
        val objects = listing.objects
        val fingerprint = listing.fingerprint

        return StorageSizeStats(
            objectCount = fingerprint.objectCount,
            totalSizeBytes = fingerprint.totalSizeBytes,
            // One object of unknown age makes the folder's age unknown, whatever the other objects report.
            newestObjectModificationTimeMillis =
                fingerprint.newestModificationTimeMillis.takeIf { objects.none { it.modificationTimeMillis <= 0 } },
        )
    }
}
