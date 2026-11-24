package com.iomete.cleanup.untrackedtablefolders.storage

/**
 * What a candidate folder holds, read from its listing alone. A folder with any table metadata file is a metadata
 * folder, and its metadata files are read before it may be deleted. Any other folder is data-only.
 */
sealed interface FolderLayout {
    /** Relative keys of the metadata files to read, never empty. A HadoopTables version-hint.text is not one of them. */
    data class Metadata(val metadataKeys: List<String>) : FolderLayout

    data class DataOnly(val shape: DataOnlyShape) : FolderLayout

    companion object {
        fun of(objects: Collection<ListedObject>): FolderLayout {
            val keys = objects.map { it.relativeKey }
            val metadataKeys = keys.filter { isMetadataFile(it.substringAfterLast('/')) }.sorted()
            if (metadataKeys.isNotEmpty()) {
                return Metadata(metadataKeys)
            }
            // A HadoopTables folder whose metadata files are gone still keeps its version hint, so it is no data-only
            // folder. With nothing to read, it is held as unreadable metadata.
            if (keys.any { it.substringAfterLast('/') == HADOOP_VERSION_HINT }) {
                return Metadata(emptyList())
            }
            return DataOnly(
                when {
                    keys.isNotEmpty() && keys.all(::isCommitterScratch) -> DataOnlyShape.COMMITTER_SCRATCH
                    keys.isNotEmpty() && keys.all { isCommitterScratch(it) || isPlainOutput(it) } -> DataOnlyShape.PLAIN_OUTPUT
                    else -> DataOnlyShape.OTHER
                },
            )
        }

        // Iceberg names a compressed metadata file v1.gz.metadata.json, and older writers v1.metadata.json.gz.
        fun isMetadataFile(name: String): Boolean = name.endsWith(".metadata.json") || name.endsWith(".metadata.json.gz")

        fun isGzipMetadataFile(name: String): Boolean = name.endsWith(".gz.metadata.json") || name.endsWith(".metadata.json.gz")

        // What a Spark or Hive commit leaves behind and no reader reads: a scratch directory at the top of the folder,
        // or a top-level _SUCCESS or checksum file. Nothing that could be another format's state or a partition.
        private fun isCommitterScratch(key: String): Boolean {
            val segments = key.split('/')
            if (segments.any { it in FORMAT_STATE_SEGMENTS || '=' in it }) {
                return false
            }
            if (segments.size == 1) {
                return segments[0] == SUCCESS_MARKER || segments[0].endsWith(CHECKSUM_SUFFIX)
            }
            val top = segments[0]
            return top == "_temporary" || top.startsWith("__magic") || top.startsWith(".spark-staging-") || top.startsWith(".hive-staging")
        }

        // Spark's file writers name each output file part-<5-digit task>-<job>..., at any depth below the folder,
        // so partition directories are allowed. Iceberg's Spark writers never use this name.
        private fun isPlainOutput(key: String): Boolean {
            val segments = key.split('/')
            if (segments.any { it in FORMAT_STATE_SEGMENTS }) {
                return false
            }
            val name = segments.last()
            return PLAIN_OUTPUT_NAME.matches(name) || name == SUCCESS_MARKER || name.endsWith(CHECKSUM_SUFFIX)
        }

        private const val HADOOP_VERSION_HINT = "version-hint.text"
        private const val SUCCESS_MARKER = "_SUCCESS"
        private const val CHECKSUM_SUFFIX = ".crc"
        private val PLAIN_OUTPUT_NAME = Regex("part-[0-9]{5}(-.*)?")

        // Directories where Delta, Spark streaming, Hudi and Parquet summary files keep table state.
        private val FORMAT_STATE_SEGMENTS = setOf("_delta_log", "_spark_metadata", ".hoodie", "_metadata", "_common_metadata")
    }
}

enum class DataOnlyShape {
    /** Only commit scratch: deleted when every other check passes. */
    COMMITTER_SCRATCH,

    /** Plain Spark output that some table or reader may still use by path; its origin cannot be proven, so it is held. */
    PLAIN_OUTPUT,

    /** Anything else, such as another engine's files or a format's state; held. */
    OTHER,
}
