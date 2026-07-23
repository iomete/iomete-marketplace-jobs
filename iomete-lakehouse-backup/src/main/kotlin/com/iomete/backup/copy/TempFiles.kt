package com.iomete.backup.copy

import com.iomete.backup.fs.FileLister
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import java.util.UUID

object TempFiles {
    // Dot-prefixed so Hadoop/Spark readers skip temp files.
    const val PREFIX = ".iomete-backup-tmp-"

    fun pathFor(finalPath: Path): Path {
        val attemptId = TaskContext.get()?.taskAttemptId()?.toString() ?: UUID.randomUUID().toString()
        val tempName = "$PREFIX$attemptId-${finalPath.name}"
        val parent = finalPath.parent
        return if (parent != null) Path(parent, tempName) else Path(tempName)
    }

    fun isTemp(name: String): Boolean = name.startsWith(PREFIX)

    fun sweep(
        fs: FileSystem,
        root: Path,
    ): Int {
        if (!fs.exists(root)) return 0

        return FileLister(fs)
            .listRecursively(root)
            .filter { isTemp(Path(it.path).name) }
            .count { fs.delete(Path(it.path), false) }
    }
}
