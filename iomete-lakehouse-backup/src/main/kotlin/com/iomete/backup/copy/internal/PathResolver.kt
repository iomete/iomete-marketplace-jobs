package com.iomete.backup.copy.internal

object PathResolver {
    fun resolveTargetPath(
        sourceFilePath: String,
        sourceRoot: String,
        targetRoot: String,
    ): String {
        // Normalize: ensure sourceRoot ends without slash for clean stripping
        val normalizedSourceRoot = sourceRoot.trimEnd('/')
        val normalizedTargetRoot = targetRoot.trimEnd('/')
        val normalizedFilePath = sourceFilePath.trimEnd('/')

        require(normalizedFilePath.startsWith(normalizedSourceRoot)) {
            "Source file path '$sourceFilePath' is not under source root '$sourceRoot'"
        }

        // Strip source root to get relative path
        val relativePath = normalizedFilePath.removePrefix(normalizedSourceRoot)

        // relativePath starts with "/" or is empty
        return if (relativePath.isEmpty()) {
            normalizedTargetRoot
        } else {
            "$normalizedTargetRoot$relativePath"
        }
    }
}
