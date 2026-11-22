package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.EtagSource
import org.apache.hadoop.fs.FileStatus
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.MessageDigest
import java.util.Arrays

/** One object of a folder listing. [relativeKey] is the object's path below the folder, without a leading slash. */
data class ListedObject(
    val relativeKey: String,
    val length: Long,
    val modificationTimeMillis: Long,
    val etag: String?,
)

/**
 * Format `fp1`, written as `fp1:` and 64 lowercase hex characters of a SHA-256 digest over:
 *
 * - `field("fp1")`, `i64(object count)`, `i64(total bytes)`, `opt(i64(newest modification time))`;
 * - then per object: `field(relative key)`, `i64(length)`, `i64(modification time)`, `opt(field(ETag))`.
 *
 * `field` is a u32 big-endian byte count and the UTF-8 bytes; `i64` is 8 bytes big-endian; `opt` is `0x00`, or `0x01`
 * and the value. Objects are sorted by the unsigned UTF-8 bytes of the relative key, then by their encoded record, so
 * listing order does not change the value. An ETag loses one pair of surrounding double quotes; an empty one counts as
 * none. Stored fingerprints are compared as strings, so any change to this encoding needs a new version prefix.
 */
data class FolderFingerprint(
    val value: String,
    val objectCount: Long,
    val totalSizeBytes: Long,
    val newestModificationTimeMillis: Long?,
) {
    companion object {
        const val VERSION = "fp1"

        fun of(objects: Collection<ListedObject>): FolderFingerprint {
            val records = objects.map { EncodedObject(it.relativeKey.toByteArray(Charsets.UTF_8), encode(it)) }
            val sorted =
                records.sortedWith { left, right ->
                    Arrays.compareUnsigned(left.key, right.key).takeIf { it != 0 } ?: Arrays.compareUnsigned(left.record, right.record)
                }
            val totalSizeBytes = objects.fold(0L) { total, item -> Math.addExact(total, item.length) }
            val newest = objects.maxOfOrNull { it.modificationTimeMillis }

            val digest = MessageDigest.getInstance("SHA-256")
            digest.update(
                bytes {
                    field(VERSION.toByteArray(Charsets.UTF_8))
                    writeLong(objects.size.toLong())
                    writeLong(totalSizeBytes)
                    optional(newest) { writeLong(it) }
                },
            )
            sorted.forEach { digest.update(it.record) }

            return FolderFingerprint(
                value = "$VERSION:" + digest.digest().joinToString("") { "%02x".format(it) },
                objectCount = objects.size.toLong(),
                totalSizeBytes = totalSizeBytes,
                newestModificationTimeMillis = newest,
            )
        }

        private fun encode(item: ListedObject): ByteArray =
            bytes {
                field(item.relativeKey.toByteArray(Charsets.UTF_8))
                writeLong(item.length)
                writeLong(item.modificationTimeMillis)
                optional(normalizeEtag(item.etag)) { field(it.toByteArray(Charsets.UTF_8)) }
            }

        private fun bytes(write: DataOutputStream.() -> Unit): ByteArray =
            ByteArrayOutputStream().also { buffer -> DataOutputStream(buffer).use { it.write() } }.toByteArray()

        private fun DataOutputStream.field(value: ByteArray) {
            writeInt(value.size)
            write(value)
        }

        private fun <T : Any> DataOutputStream.optional(
            value: T?,
            write: DataOutputStream.(T) -> Unit,
        ) {
            if (value == null) {
                writeByte(0)
            } else {
                writeByte(1)
                write(value)
            }
        }
    }

    private class EncodedObject(
        val key: ByteArray,
        val record: ByteArray,
    )
}

/** The status's ETag when the filesystem exposes one, without surrounding quotes. */
fun etagOf(status: FileStatus): String? = normalizeEtag((status as? EtagSource)?.etag)

internal fun normalizeEtag(etag: String?): String? = etag?.removeSurrounding("\"")?.takeIf { it.isNotEmpty() }
