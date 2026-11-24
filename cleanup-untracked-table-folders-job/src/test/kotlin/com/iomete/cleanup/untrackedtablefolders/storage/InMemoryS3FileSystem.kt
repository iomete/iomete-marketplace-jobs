package com.iomete.cleanup.untrackedtablefolders.storage

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.EtagSource
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import java.io.IOException

/** Test filesystem registered under `fs.s3a.impl`; content is keyed by `fs.s3a.endpoint`. */
class InMemoryS3FileSystem : FileSystem() {

    data class Entry(
        val isDirectory: Boolean,
        val length: Long,
        val modificationTimeMillis: Long,
        val etag: String? = null,
    )

    data class Opened(
        val endpoint: String,
        val accessKey: String?,
        val pathStyleAccess: String?,
        val sslEnabled: String?,
        val region: String?,
    )

    companion object {
        const val UNSET_ENDPOINT = "<unset>"

        /** endpoint -> (absolute path -> entry) */
        val stores = ConcurrentHashMap<String, MutableMap<String, Entry>>()

        /** endpoint -> (absolute path -> bytes) for objects that [open] reads; an object without bytes reads as empty. */
        val contents = ConcurrentHashMap<String, MutableMap<String, ByteArray>>()
        val openCalls = CopyOnWriteArrayList<String>()
        val opened = CopyOnWriteArrayList<Opened>()
        val closedEndpoints = CopyOnWriteArrayList<String>()

        @Volatile
        var failDeleteFor: String? = null

        @Volatile
        var refuseDeleteFor: String? = null

        @Volatile
        var failListFor: String? = null

        /** path -> error thrown by listStatus, whether or not the path exists */
        val listFailures = ConcurrentHashMap<String, IOException>()

        /** path -> error thrown by getFileStatus, whether or not the path exists */
        val statusFailures = ConcurrentHashMap<String, IOException>()

        /** Runs at the start of delete with the target path, to simulate a change between exists() and delete(). */
        @Volatile
        var beforeDelete: ((String) -> Unit)? = null

        /** Runs at the start of getFileStatus with the target path, to simulate a change between a listing and a status read. */
        @Volatile
        var beforeGetFileStatus: ((String) -> Unit)? = null

        /** Runs at the start of listStatus with the listed path, after the call is counted. */
        @Volatile
        var beforeListStatus: ((String) -> Unit)? = null

        /** Scheme the filesystem reports, to stand in for a non-S3A implementation. */
        @Volatile
        var reportedScheme: String = "s3a"

        /** Listings that start at a path, by listStatus or listFiles; listFiles counts once for its root. */
        val listStatusCalls = ConcurrentHashMap<String, Int>()
        val listFilesCalls = ConcurrentHashMap<String, Int>()
        val getFileStatusCalls = ConcurrentHashMap<String, Int>()

        /** Every delete call in order, as (path, recursive). */
        val deleteCalls = CopyOnWriteArrayList<Pair<String, Boolean>>()

        /** Statuses handed out by listFiles iterators, by root, to show where a listing stopped. */
        val listFilesReturned = ConcurrentHashMap<String, Int>()

        /**
         * Runs after listFiles has taken its snapshot and before it returns, with the root and the call number for that
         * root. A change made here is not in that listing but is seen by every later call: the gap between a listing
         * and the first delete.
         */
        @Volatile
        var afterListFiles: ((String, Int) -> Unit)? = null

        /** root -> existing paths that listFiles(root) also returns, as a store that maps keys outside the root into it. */
        val foreignListingEntries = ConcurrentHashMap<String, List<String>>()

        /** Like S3 listings, wrap ETags in double quotes in listFiles, while getFileStatus returns them bare. */
        @Volatile
        var quoteListingEtags: Boolean = false

        /** Like an S3 HEAD, getFileStatus reports modification times to the second; listings keep milliseconds. */
        @Volatile
        var statusTimesInSeconds: Boolean = false

        /** Like S3A, a non-recursive delete of a non-empty directory throws instead of returning false. */
        @Volatile
        var nonEmptyDirectoryDeleteThrows: Boolean = false

        /** Like S3A, deleting the last entry of a directory leaves a marker for that directory. */
        @Volatile
        var createParentMarkerOnDelete: Boolean = false

        @Volatile
        var failCloseFor: String? = null

        /** Like S3A, a prefix that holds objects is a directory even without a marker object for it. */
        @Volatile
        var impliedDirectories: Boolean = false

        fun reset() {
            stores.clear()
            contents.clear()
            openCalls.clear()
            opened.clear()
            closedEndpoints.clear()
            failDeleteFor = null
            refuseDeleteFor = null
            failListFor = null
            listFailures.clear()
            statusFailures.clear()
            beforeDelete = null
            beforeGetFileStatus = null
            beforeListStatus = null
            reportedScheme = "s3a"
            listStatusCalls.clear()
            listFilesCalls.clear()
            getFileStatusCalls.clear()
            deleteCalls.clear()
            listFilesReturned.clear()
            afterListFiles = null
            foreignListingEntries.clear()
            quoteListingEtags = false
            statusTimesInSeconds = false
            nonEmptyDirectoryDeleteThrows = false
            createParentMarkerOnDelete = false
            failCloseFor = null
            impliedDirectories = false
        }

        fun store(endpoint: String): MutableMap<String, Entry> =
            stores.getOrPut(endpoint) { ConcurrentHashMap() }

        fun putDirectory(
            endpoint: String,
            path: String,
            modificationTimeMillis: Long = 0L,
        ) {
            store(endpoint)[normalize(path)] = Entry(true, 0, modificationTimeMillis)
        }

        fun putFile(
            endpoint: String,
            path: String,
            length: Long,
            modificationTimeMillis: Long = 1L,
            etag: String? = null,
        ) {
            store(endpoint)[normalize(path)] = Entry(false, length, modificationTimeMillis, etag)
        }

        /** Replaces an existing object, as a new write to the same key does. */
        fun overwriteFile(
            endpoint: String,
            path: String,
            length: Long,
            modificationTimeMillis: Long,
            etag: String? = null,
        ) {
            val key = normalize(path)
            check(store(endpoint)[key]?.isDirectory == false) { "no object to overwrite at $key" }
            store(endpoint)[key] = Entry(false, length, modificationTimeMillis, etag)
        }

        fun paths(endpoint: String): Set<String> = store(endpoint).keys.toSet()

        fun putFileWithContent(
            endpoint: String,
            path: String,
            content: ByteArray,
            modificationTimeMillis: Long = 1L,
        ) {
            putFile(endpoint, path, content.size.toLong(), modificationTimeMillis)
            contents.getOrPut(endpoint) { ConcurrentHashMap() }[normalize(path)] = content
        }

        fun normalize(path: String): String = path.trimEnd('/')
    }

    private lateinit var fsUri: URI
    private var endpoint: String = UNSET_ENDPOINT
    private var workingDirectory: Path = Path("/")

    override fun initialize(
        name: URI,
        conf: Configuration,
    ) {
        super.initialize(name, conf)
        fsUri = URI(name.scheme, name.authority, null, null, null)
        endpoint = conf.get(CatalogStorageProperties.FS_ENDPOINT) ?: UNSET_ENDPOINT
        workingDirectory = Path(fsUri)
        opened +=
            Opened(
                endpoint = endpoint,
                accessKey = conf.get(CatalogStorageProperties.FS_ACCESS_KEY),
                pathStyleAccess = conf.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS),
                sslEnabled = conf.get(CatalogStorageProperties.FS_SSL_ENABLED),
                region = conf.get(CatalogStorageProperties.FS_REGION),
            )
    }

    override fun close() {
        closedEndpoints += endpoint
        super.close()
        if (endpoint == failCloseFor) {
            throw IOException("simulated close failure for $endpoint")
        }
    }

    override fun getScheme(): String = reportedScheme

    override fun getUri(): URI = fsUri

    override fun getWorkingDirectory(): Path = workingDirectory

    override fun setWorkingDirectory(newDir: Path) {
        workingDirectory = newDir
    }

    private fun key(path: Path): String = normalize(path.toString())

    private fun entries(): MutableMap<String, Entry> = store(endpoint)

    override fun getFileStatus(path: Path): FileStatus {
        val target = key(path)
        getFileStatusCalls.merge(target, 1, Int::plus)
        beforeGetFileStatus?.invoke(target)
        statusFailures[target]?.let { throw it }
        val entry =
            entries()[target]
                ?: impliedDirectory(target)
                ?: throw FileNotFoundException("No such path: $target")
        val reported = if (statusTimesInSeconds) entry.copy(modificationTimeMillis = Math.floorDiv(entry.modificationTimeMillis, 1000L) * 1000L) else entry
        return toStatus(target, reported)
    }

    private fun impliedDirectory(path: String): Entry? =
        Entry(true, 0, 0L).takeIf { impliedDirectories && entries().keys.any { it.startsWith("$path/") } }

    override fun listStatus(path: Path): Array<FileStatus> {
        val parent = key(path)
        listStatusCalls.merge(parent, 1, Int::plus)
        beforeListStatus?.invoke(parent)
        if (parent == failListFor) {
            throw java.io.IOException("simulated listing failure: $parent")
        }
        listFailures[parent]?.let { throw it }
        val parentEntry = entries()[parent] ?: impliedDirectory(parent) ?: throw FileNotFoundException("No such path: $parent")
        // As the FileSystem contract says, listing an object returns that object.
        if (!parentEntry.isDirectory) return arrayOf(toStatus(parent, parentEntry))

        val explicitChildren =
            entries().filterKeys { it != parent && it.startsWith("$parent/") && !it.removePrefix("$parent/").contains('/') }
        val impliedChildren =
            if (impliedDirectories) {
                entries().keys
                    .filter { it.startsWith("$parent/") }
                    .map { "$parent/" + it.removePrefix("$parent/").substringBefore('/') }
                    .filterNot { it in explicitChildren }
                    .distinct()
                    .associateWith { Entry(true, 0, 0L) }
            } else {
                emptyMap()
            }

        return (explicitChildren + impliedChildren)
            .map { (childPath, entry) -> toStatus(childPath, entry) }
            .sortedBy { it.path.toString() }
            .toTypedArray()
    }

    override fun listFiles(
        path: Path,
        recursive: Boolean,
    ): RemoteIterator<LocatedFileStatus> {
        val root = key(path)
        listStatusCalls.merge(root, 1, Int::plus)
        val call = listFilesCalls.merge(root, 1, Int::plus)!!
        if (root == failListFor) {
            throw java.io.IOException("simulated listing failure: $root")
        }
        listFailures[root]?.let { throw it }
        val rootEntry = entries()[root] ?: impliedDirectory(root) ?: throw FileNotFoundException("No such path: $root")

        val listed =
            if (!rootEntry.isDirectory) {
                listOf(root to rootEntry)
            } else {
                val keys =
                    entries().filter { (childPath, entry) ->
                        !entry.isDirectory && childPath.startsWith("$root/") && (recursive || !childPath.removePrefix("$root/").contains('/'))
                    }.keys.sorted() + foreignListingEntries[root].orEmpty()
                keys.map { it to entries().getValue(it) }
            }
        val statuses = listed.map { (childPath, entry) -> locatedStatus(childPath, entry) }
        afterListFiles?.invoke(root, call)

        val iterator = statuses.iterator()
        return object : RemoteIterator<LocatedFileStatus> {
            override fun hasNext(): Boolean = iterator.hasNext()

            override fun next(): LocatedFileStatus = iterator.next().also { listFilesReturned.merge(root, 1, Int::plus) }
        }
    }

    override fun delete(
        path: Path,
        recursive: Boolean,
    ): Boolean {
        val target = key(path)
        deleteCalls += target to recursive
        beforeDelete?.invoke(target)
        if (target == failDeleteFor) {
            throw java.io.IOException("simulated delete failure: $target")
        }
        if (target == refuseDeleteFor) return false
        val entry = entries()[target]
        val children = entries().keys.filter { it.startsWith("$target/") }
        if (!recursive) {
            // An object is deleted alone even when keys below its name exist; a directory only when it is empty.
            when {
                entry != null && !entry.isDirectory -> entries().remove(target)
                children.isNotEmpty() ->
                    if (nonEmptyDirectoryDeleteThrows) throw PathIsNotEmptyDirectoryException(target) else return false
                entry == null -> return false
                else -> entries().remove(target)
            }
            leaveParentMarker(target)
            return true
        }
        val victims = listOfNotNull(target.takeIf { entry != null }) + children
        if (victims.isEmpty()) return false
        victims.forEach { entries().remove(it) }
        return true
    }

    private fun leaveParentMarker(deleted: String) {
        if (!createParentMarkerOnDelete) return
        val parentPath = Path(deleted).parent ?: return
        if (parentPath.isRoot) return
        val parent = key(parentPath)
        if (entries().keys.none { it == parent || it.startsWith("$parent/") }) {
            entries()[parent] = Entry(true, 0, 0L)
        }
    }

    private fun toStatus(
        path: String,
        entry: Entry,
    ): FileStatus {
        val status =
            FileStatus(
                entry.length,
                entry.isDirectory,
                1,
                BLOCK_SIZE,
                entry.modificationTimeMillis,
                Path(path),
            )
        return entry.etag?.let { EtagFileStatus(status, it) } ?: status
    }

    private fun locatedStatus(
        path: String,
        entry: Entry,
    ): LocatedFileStatus {
        val status = FileStatus(entry.length, entry.isDirectory, 1, BLOCK_SIZE, entry.modificationTimeMillis, Path(path))
        val etag = entry.etag?.let { if (quoteListingEtags) "\"$it\"" else it }
        return etag?.let { EtagLocatedFileStatus(status, it) } ?: LocatedFileStatus(status, null)
    }

    override fun open(
        path: Path,
        bufferSize: Int,
    ): FSDataInputStream {
        val target = key(path)
        openCalls += target
        val entry = entries()[target] ?: throw FileNotFoundException("No such path: $target")
        if (entry.isDirectory) throw FileNotFoundException("Not a file: $target")
        return FSDataInputStream(SeekableBytes(contents[endpoint]?.get(target) ?: ByteArray(0)))
    }

    private class SeekableBytes(
        private val bytes: ByteArray,
    ) : java.io.InputStream(), org.apache.hadoop.fs.Seekable, org.apache.hadoop.fs.PositionedReadable {
        private var position = 0

        override fun read(): Int = if (position < bytes.size) bytes[position++].toInt() and 0xFF else -1

        override fun seek(pos: Long) {
            position = pos.toInt()
        }

        override fun getPos(): Long = position.toLong()

        override fun seekToNewSource(targetPos: Long): Boolean = false

        override fun read(
            position: Long,
            buffer: ByteArray,
            offset: Int,
            length: Int,
        ): Int {
            if (position >= bytes.size) return -1
            val count = minOf(length, bytes.size - position.toInt())
            System.arraycopy(bytes, position.toInt(), buffer, offset, count)
            return count
        }

        override fun readFully(
            position: Long,
            buffer: ByteArray,
            offset: Int,
            length: Int,
        ) {
            if (read(position, buffer, offset, length) != length) throw java.io.EOFException()
        }

        override fun readFully(
            position: Long,
            buffer: ByteArray,
        ) = readFully(position, buffer, 0, buffer.size)
    }

    override fun create(
        path: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable?,
    ): FSDataOutputStream = throw UnsupportedOperationException("create is not needed by this job")

    override fun append(
        path: Path,
        bufferSize: Int,
        progress: Progressable?,
    ): FSDataOutputStream = throw UnsupportedOperationException("append is not needed by this job")

    override fun rename(
        src: Path,
        dst: Path,
    ): Boolean = throw UnsupportedOperationException("rename is not needed by this job")

    override fun mkdirs(
        path: Path,
        permission: FsPermission,
    ): Boolean {
        putDirectory(endpoint, key(path))
        return true
    }
}

private const val BLOCK_SIZE = 32L * 1024 * 1024

private class EtagFileStatus(
    status: FileStatus,
    private val etag: String,
) : FileStatus(status),
    EtagSource {
    override fun getEtag(): String = etag
}

private class EtagLocatedFileStatus(
    status: FileStatus,
    private val etag: String,
) : LocatedFileStatus(status, null),
    EtagSource {
    override fun getEtag(): String = etag
}
