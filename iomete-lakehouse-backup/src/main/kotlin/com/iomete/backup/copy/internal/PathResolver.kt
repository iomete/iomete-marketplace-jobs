package com.iomete.backup.copy.internal

object PathResolver {
    fun resolveTargetPath(
        sourceFilePath: String,
        sourceRoot: String,
        targetRoot: String,
    ): String {
        val normalizedTargetRoot = targetRoot.trimEnd('/')
        val relativePath = relativize(sourceFilePath, sourceRoot)

        return if (relativePath.isEmpty()) {
            normalizedTargetRoot
        } else {
            "$normalizedTargetRoot/$relativePath"
        }
    }

    fun relativize(
        path: String,
        root: String,
    ): String {
        val normalizedRoot = root.trimEnd('/')
        val normalizedPath = path.trimEnd('/')

        require(normalizedPath == normalizedRoot || normalizedPath.startsWith("$normalizedRoot/")) {
            "Path '$path' is not under root '$root'"
        }

        return normalizedPath.removePrefix(normalizedRoot).trimStart('/')
    }
}
