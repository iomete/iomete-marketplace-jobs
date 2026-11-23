package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.EtagSource
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FolderFingerprintTest {

    private val golden =
        listOf(
            ListedObject("part-00000.parquet", 1024, 1_700_000_000_123, "\"9b2cf535f27731c974343645a3985328\""),
            ListedObject("date=2024-01-01/part-00001.parquet", 2048, 1_700_000_001_000, null),
            ListedObject("_SUCCESS", 0, 1_700_000_002_000, "d41d8cd98f00b204e9800998ecf8427e"),
            ListedObject("\u00fc/na\u00efve name.parquet", 7, 1_700_000_003_000, "abc-2"),
        )

    // Computed outside the job from the encoding in the FolderFingerprint KDoc, so PR 2 can store and compare these.
    @Test
    fun `the fp1 format is pinned`() {
        val fingerprint = FolderFingerprint.of(golden)

        assertEquals("fp1:15c21b89010503986cfda88be9ce77b3a908b99f96301dfb7eafa90906ec97dc", fingerprint.value)
        assertEquals(4L, fingerprint.objectCount)
        assertEquals(3079L, fingerprint.totalSizeBytes)
        assertEquals(1_700_000_003_000, fingerprint.newestModificationTimeMillis)
        assertEquals("fp1:f21d1162033b85ae3384497a4630f9e3b20c20c29bc79a9e10cc22c7a3ec4642", FolderFingerprint.of(emptyList()).value)
        assertNull(FolderFingerprint.of(emptyList()).newestModificationTimeMillis)
        assertTrue(Regex("fp1:[0-9a-f]{64}").matches(fingerprint.value))
    }

    @Test
    fun `listing order does not change the fingerprint`() {
        val expected = FolderFingerprint.of(golden).value

        assertEquals(expected, FolderFingerprint.of(golden.reversed()).value)
        assertEquals(expected, FolderFingerprint.of(golden.sortedBy { it.length }).value)
        assertEquals(expected, FolderFingerprint.of(listOf(golden[2], golden[0], golden[3], golden[1])).value)
    }

    @Test
    fun `any single field change gives a different fingerprint`() {
        val base = FolderFingerprint.of(golden).value
        val first = golden[0]
        val variants =
            mapOf(
                "key" to first.copy(relativeKey = "part-00000.parquet2"),
                "length" to first.copy(length = 1025),
                "time by one millisecond" to first.copy(modificationTimeMillis = 1_700_000_000_124),
                "etag" to first.copy(etag = "9b2cf535f27731c974343645a3985329"),
                "etag removed" to first.copy(etag = null),
            )

        variants.forEach { (field, changed) ->
            assertNotEquals(base, FolderFingerprint.of(listOf(changed) + golden.drop(1)).value, field)
        }
        assertNotEquals(base, FolderFingerprint.of(golden.drop(1)).value, "object removed")
        assertNotEquals(base, FolderFingerprint.of(golden + ListedObject("late.parquet", 1, 1, null)).value, "object added")
        assertNotEquals(
            FolderFingerprint.of(listOf(ListedObject("a", 1, 1, null))).value,
            FolderFingerprint.of(listOf(ListedObject("a", 1, 1, "x"))).value,
            "etag added to an object that had none",
        )
    }

    @Test
    fun `fields cannot run into each other`() {
        // Without the byte counts these two encode to the same bytes: the key of one swallows the length, time and
        // ETag flag of the other, whose ETag holds the rest.
        val one = "\u0000".repeat(7) + "\u0001"
        val swallowing = ListedObject("a$one$one\u0001", 1, 1, null)
        val swallowed = ListedObject("a", 1, 1, "$one$one\u0000")
        assertNotEquals(FolderFingerprint.of(listOf(swallowing)).value, FolderFingerprint.of(listOf(swallowed)).value)

        val pairs =
            listOf(
                listOf(ListedObject("ab", 1, 1, null)) to listOf(ListedObject("a", 1, 1, null), ListedObject("b", 0, 1, null)),
                listOf(ListedObject("a", 1, 1, "bc")) to listOf(ListedObject("a", 1, 1, "b"), ListedObject("c", 0, 1, null)),
                listOf(ListedObject("a\u0000", 1, 1, null)) to listOf(ListedObject("a", 1, 1, "\u0000")),
                listOf(ListedObject("a/b", 1, 1, null)) to listOf(ListedObject("a", 1, 1, "/b")),
                listOf(ListedObject("a\nb", 1, 1, null)) to listOf(ListedObject("a", 1, 1, null), ListedObject("b", 0, 1, null)),
            )

        pairs.forEach { (left, right) ->
            assertNotEquals(FolderFingerprint.of(left).value, FolderFingerprint.of(right).value, "$left vs $right")
        }
    }

    @Test
    fun `an etag keeps one meaning whatever its quoting`() {
        val bare = FolderFingerprint.of(listOf(ListedObject("a", 1, 1, "abc"))).value

        assertEquals(bare, FolderFingerprint.of(listOf(ListedObject("a", 1, 1, "\"abc\""))).value)
        assertEquals(
            FolderFingerprint.of(listOf(ListedObject("a", 1, 1, null))).value,
            FolderFingerprint.of(listOf(ListedObject("a", 1, 1, ""))).value,
            "an empty etag counts as none",
        )
        assertEquals("abc", normalizeEtag("\"abc\""))
        assertEquals("\"abc", normalizeEtag("\"abc"))
        assertNull(normalizeEtag("\"\""))
    }

    @Test
    fun `the etag is read only from an EtagSource status`() {
        val plain = FileStatus(1, false, 1, 1, 1, Path("s3a://b/db/t/a"))
        val tagged =
            object : FileStatus(plain), EtagSource {
                override fun getEtag(): String = "\"0123\""
            }

        assertNull(etagOf(plain))
        assertEquals("0123", etagOf(tagged))
    }

    @Test
    fun `objects that share a key are encoded in one order`() {
        val one = ListedObject("a", 1, 1, null)
        val two = ListedObject("a", 2, 1, null)

        assertEquals(FolderFingerprint.of(listOf(one, two)).value, FolderFingerprint.of(listOf(two, one)).value)
        assertNotEquals(FolderFingerprint.of(listOf(one)).value, FolderFingerprint.of(listOf(one, one)).value)
    }

    @Test
    fun `keys sort by UTF-8 bytes, not by UTF-16 units`() {
        // U+FF5E sorts after U+1F600 in UTF-16 but before it in UTF-8, so a UTF-16 sort would change the digest.
        val objects = listOf(ListedObject("\uFF5E", 1, 1, null), ListedObject("\uD83D\uDE00", 1, 1, null))

        assertEquals(FolderFingerprint.of(objects).value, FolderFingerprint.of(objects.reversed()).value)
        assertEquals("fp1:" + referenceDigestInUtf8Order(objects), FolderFingerprint.of(objects).value)
    }

    @Test
    fun `a total size that does not fit in a long fails instead of wrapping`() {
        assertThrows(ArithmeticException::class.java) {
            FolderFingerprint.of(listOf(ListedObject("a", Long.MAX_VALUE, 1, null), ListedObject("b", 1, 1, null)))
        }
    }

    private fun referenceDigestInUtf8Order(objects: List<ListedObject>): String {
        fun field(value: ByteArray) = java.nio.ByteBuffer.allocate(4).putInt(value.size).array() + value

        fun long(value: Long) = java.nio.ByteBuffer.allocate(8).putLong(value).array()

        val records =
            objects
                .map { it.relativeKey.toByteArray(Charsets.UTF_8) to (field(it.relativeKey.toByteArray()) + long(it.length) + long(it.modificationTimeMillis) + byteArrayOf(0)) }
                .sortedWith { a, b -> java.util.Arrays.compareUnsigned(a.first, b.first) }
        val digest = java.security.MessageDigest.getInstance("SHA-256")
        digest.update(field("fp1".toByteArray()) + long(objects.size.toLong()) + long(objects.sumOf { it.length }) + byteArrayOf(1) + long(1))
        records.forEach { digest.update(it.second) }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }
}
