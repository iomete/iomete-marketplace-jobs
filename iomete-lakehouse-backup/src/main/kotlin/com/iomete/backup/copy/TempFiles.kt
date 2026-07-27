package com.iomete.backup.copy

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
}
