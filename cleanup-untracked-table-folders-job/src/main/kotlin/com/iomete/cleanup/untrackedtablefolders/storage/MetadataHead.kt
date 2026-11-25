package com.iomete.cleanup.untrackedtablefolders.storage

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.GZIPInputStream

/** What the metadata files of one folder say about it. Anything but [OWN_TABLE] holds the folder. */
enum class MetadataVerdict(val code: String) {
    /** Every metadata file is a table's, with a table UUID and the folder itself as its location. */
    OWN_TABLE("own_table"),

    /** A view keeps its metadata here. A view can read any path, so its folder is never deleted on its own. */
    VIEW("metadata_view"),
    UNREADABLE("metadata_unreadable"),
    NO_UUID("metadata_no_uuid"),

    /** A metadata file names another location, so the table it belongs to may live elsewhere and read this one. */
    LOCATION_MISMATCH("metadata_location_mismatch"),
}

/** The verdict for a folder's metadata files, and the table UUIDs they name when it is [MetadataVerdict.OWN_TABLE]. */
data class FolderMetadata(
    val verdict: MetadataVerdict,
    val tableUuids: Set<String> = emptySet(),
)

internal object MetadataHead {
    // Iceberg writes the UUID and the location first, so a head this size holds them unless the file is not Iceberg's.
    const val MAX_HEAD_BYTES = 1L shl 20

    private val jsonFactory = JsonFactory()

    /** Reads the top-level fields of one metadata file until it knows the verdict. Throws when the head cannot be read. */
    fun verdict(
        input: InputStream,
        gzip: Boolean,
        folderPath: String,
    ): MetadataVerdict = read(input, gzip, folderPath).verdict

    /** As [verdict], with the table UUID when the file is this folder's own table's. */
    fun read(
        input: InputStream,
        gzip: Boolean,
        folderPath: String,
    ): FolderMetadata {
        val stream = BoundedStream(if (gzip) GZIPInputStream(input) else input, MAX_HEAD_BYTES)
        var tableUuid: String? = null
        var location: String? = null
        jsonFactory.createParser(stream).use { parser ->
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return FolderMetadata(MetadataVerdict.UNREADABLE)
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                val name = parser.currentName()
                val value = if (parser.nextToken() == JsonToken.VALUE_STRING) parser.text else null.also { parser.skipChildren() }
                when (name) {
                    "view-uuid" -> return FolderMetadata(MetadataVerdict.VIEW)
                    "table-uuid" -> tableUuid = value?.takeIf { it.isNotBlank() }
                    "location" -> location = value
                }
                if (tableUuid != null && location != null) {
                    break
                }
            }
        }
        return when {
            tableUuid == null -> FolderMetadata(MetadataVerdict.NO_UUID)
            !sameLocation(location, folderPath) -> FolderMetadata(MetadataVerdict.LOCATION_MISMATCH)
            else -> FolderMetadata(MetadataVerdict.OWN_TABLE, setOfNotNull(tableUuid))
        }
    }

    private fun sameLocation(
        location: String?,
        folderPath: String,
    ): Boolean =
        location != null &&
            runCatching { StoragePathUtils.normalizeLocation(location) == StoragePathUtils.normalizeLocation(folderPath) }.getOrDefault(false)

    private class BoundedStream(
        input: InputStream,
        private val maxBytes: Long,
    ) : FilterInputStream(input) {
        private var read = 0L

        override fun read(): Int = super.read().also { if (it >= 0) count(1) }

        override fun read(
            buffer: ByteArray,
            offset: Int,
            length: Int,
        ): Int = super.read(buffer, offset, length).also { if (it > 0) count(it.toLong()) }

        private fun count(bytes: Long) {
            read += bytes
            if (read > maxBytes) throw IOException("metadata file head is larger than $maxBytes bytes")
        }
    }
}
