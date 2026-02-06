package com.iomete.backup.fs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Recursively lists files under a given path using Hadoop [FileSystem].
 *
 * Works transparently with any Hadoop-compatible filesystem (S3A, HDFS/Isilon, local, etc.)
 * because [FileSystem] is the Hadoop abstraction layer -- the URI scheme determines the
 * concrete implementation at runtime.
 *
 * @property fileSystem The Hadoop FileSystem instance to use for listing.
 */
class FileLister(private val fileSystem: FileSystem) {

    /**
     * Recursively list all files under [rootPath].
     *
     * Returns a lazy [Sequence] of [FileEntry] containing each file's
     * full path, size in bytes, and last modification time (epoch millis).
     *
     * @param rootPath The root directory to list recursively.
     * @return A sequence of [FileEntry] for every file found under [rootPath].
     */
    fun listRecursively(rootPath: Path): Sequence<FileEntry> {
        val iterator = fileSystem.listFiles(rootPath, true)
        return generateSequence {
            if (iterator.hasNext()) {
                val status = iterator.next()
                FileEntry(
                    path = status.path.toString(),
                    size = status.len,
                    modificationTime = status.modificationTime
                )
            } else {
                null
            }
        }
    }
}
