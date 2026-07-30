package com.iomete.backup.fs

import com.iomete.backup.config.StorageConfig
import com.iomete.backup.model.FileEntry
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

fun <T> useFileLister(
    config: StorageConfig,
    root: String,
    block: (FileLister) -> T,
): T =
    FileSystemFactory.create(config, URI(root), HadoopConfigBuilder.build(config)).use {
        block(FileLister(it))
    }

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

    fun listLeafEmptyDirectories(rootPath: Path): List<Path> =
        fileSystem
            .listStatus(rootPath)
            .filter { it.isDirectory }
            .flatMap { leafEmptyDirectories(it.path) }

    private fun leafEmptyDirectories(path: Path): List<Path> {
        val children = fileSystem.listStatus(path)
        if (children.isEmpty()) return listOf(path)

        return children
            .filter { it.isDirectory }
            .flatMap { leafEmptyDirectories(it.path) }
    }
}
