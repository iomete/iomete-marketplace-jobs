package com.iomete.cleanup.untrackedtablefolders.storage

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/** Test filesystem registered under `fs.s3a.impl`; content is keyed by `fs.s3a.endpoint`. */
class InMemoryS3FileSystem : FileSystem() {

    data class Entry(
        val isDirectory: Boolean,
        val length: Long,
        val modificationTimeMillis: Long,
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
        val opened = CopyOnWriteArrayList<Opened>()
        val closedEndpoints = CopyOnWriteArrayList<String>()

        @Volatile
        var failDeleteFor: String? = null

        fun reset() {
            stores.clear()
            opened.clear()
            closedEndpoints.clear()
            failDeleteFor = null
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
        ) {
            store(endpoint)[normalize(path)] = Entry(false, length, 0)
        }

        fun paths(endpoint: String): Set<String> = store(endpoint).keys.toSet()

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
    }

    override fun getScheme(): String = "s3a"

    override fun getUri(): URI = fsUri

    override fun getWorkingDirectory(): Path = workingDirectory

    override fun setWorkingDirectory(newDir: Path) {
        workingDirectory = newDir
    }

    private fun key(path: Path): String = normalize(path.toString())

    private fun entries(): MutableMap<String, Entry> = store(endpoint)

    override fun getFileStatus(path: Path): FileStatus {
        val target = key(path)
        val entry = entries()[target] ?: throw FileNotFoundException("No such path: $target")
        return toStatus(target, entry)
    }

    override fun listStatus(path: Path): Array<FileStatus> {
        val parent = key(path)
        entries()[parent] ?: throw FileNotFoundException("No such path: $parent")

        return entries()
            .filterKeys { it != parent && it.startsWith("$parent/") && !it.removePrefix("$parent/").contains('/') }
            .map { (childPath, entry) -> toStatus(childPath, entry) }
            .sortedBy { it.path.toString() }
            .toTypedArray()
    }

    override fun delete(
        path: Path,
        recursive: Boolean,
    ): Boolean {
        val target = key(path)
        if (target == failDeleteFor) {
            throw java.io.IOException("simulated delete failure: $target")
        }
        val victims = entries().keys.filter { it == target || it.startsWith("$target/") }
        if (victims.isEmpty()) return false
        if (!recursive && victims.size > 1) return false
        victims.forEach { entries().remove(it) }
        return true
    }

    private fun toStatus(
        path: String,
        entry: Entry,
    ): FileStatus =
        FileStatus(
            entry.length,
            entry.isDirectory,
            1,
            BLOCK_SIZE,
            entry.modificationTimeMillis,
            Path(path),
        )

    override fun open(
        path: Path,
        bufferSize: Int,
    ): FSDataInputStream = throw UnsupportedOperationException("open is not needed by this job")

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
