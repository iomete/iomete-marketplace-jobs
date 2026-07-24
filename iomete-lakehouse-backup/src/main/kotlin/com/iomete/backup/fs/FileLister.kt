package com.iomete.backup.fs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class FileLister(
    private val fileSystem: FileSystem,
) {
    fun listRecursively(rootPath: Path): Sequence<FileEntry> {
        val iterator = fileSystem.listFiles(rootPath, true)

        return generateSequence {
            if (iterator.hasNext()) {
                val status = iterator.next()
                FileEntry(
                    path = status.path.toString(),
                    size = status.len,
                    modificationTime = status.modificationTime,
                )
            } else {
                null
            }
        }
    }

    fun listLeafEmptyDirectories(rootPath: Path): List<Path> {
        val children = fileSystem.listStatus(rootPath)
        if (children.isEmpty()) return listOf(rootPath)

        return children
            .filter { it.isDirectory }
            .flatMap { listLeafEmptyDirectories(it.path) }
    }
}
