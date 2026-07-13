package com.iomete.backup.fs

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FileListerTest {
    private lateinit var fileSystem: FileSystem
    private lateinit var fileLister: FileLister

    @BeforeEach
    fun setup() {
        fileSystem = mockk(relaxed = true)
        fileLister = FileLister(fileSystem)
    }

    @Test
    fun `empty directory returns empty sequence`() {
        val root = Path("s3a://bucket/empty-dir")
        every { fileSystem.listFiles(root, true) } returns emptyRemoteIterator()

        val result = fileLister.listRecursively(root).toList()

        assertTrue(result.isEmpty())
    }

    @Test
    fun `single file returns one FileEntry with correct fields`() {
        val root = Path("s3a://bucket/data")
        val status =
            mockFileStatus(
                path = "s3a://bucket/data/file1.parquet",
                size = 1024L,
                mtime = 1700000000000L,
            )
        every { fileSystem.listFiles(root, true) } returns remoteIteratorOf(status)

        val result = fileLister.listRecursively(root).toList()

        assertEquals(1, result.size)
        assertEquals("s3a://bucket/data/file1.parquet", result[0].path)
        assertEquals(1024L, result[0].size)
        assertEquals(1700000000000L, result[0].modificationTime)
    }

    @Test
    fun `multiple files in nested directories are all returned`() {
        val root = Path("hdfs://namenode:8020/warehouse")
        val statuses =
            listOf(
                mockFileStatus("hdfs://namenode:8020/warehouse/db/table/part-0001.parquet", 500L, 1700000000000L),
                mockFileStatus("hdfs://namenode:8020/warehouse/db/table/part-0002.parquet", 750L, 1700000001000L),
                mockFileStatus("hdfs://namenode:8020/warehouse/db/table/metadata/snap-001.avro", 200L, 1700000002000L),
            )
        every { fileSystem.listFiles(root, true) } returns remoteIteratorOf(*statuses.toTypedArray())

        val result = fileLister.listRecursively(root).toList()

        assertEquals(3, result.size)
        assertEquals("hdfs://namenode:8020/warehouse/db/table/part-0001.parquet", result[0].path)
        assertEquals(500L, result[0].size)
        assertEquals("hdfs://namenode:8020/warehouse/db/table/part-0002.parquet", result[1].path)
        assertEquals(750L, result[1].size)
        assertEquals("hdfs://namenode:8020/warehouse/db/table/metadata/snap-001.avro", result[2].path)
        assertEquals(200L, result[2].size)
        assertEquals(1700000002000L, result[2].modificationTime)
    }

    @Test
    fun `path captures full URI including scheme`() {
        val root = Path("s3a://my-bucket/prefix")
        val status =
            mockFileStatus(
                path = "s3a://my-bucket/prefix/nested/deep/file.csv",
                size = 42L,
                mtime = 1600000000000L,
            )
        every { fileSystem.listFiles(root, true) } returns remoteIteratorOf(status)

        val result = fileLister.listRecursively(root).toList()

        assertTrue(result[0].path.startsWith("s3a://"))
        assertEquals("s3a://my-bucket/prefix/nested/deep/file.csv", result[0].path)
    }

    @Test
    fun `calls FileSystem listFiles with recursive flag`() {
        val root = Path("s3a://bucket/dir")
        every { fileSystem.listFiles(root, true) } returns emptyRemoteIterator()

        fileLister.listRecursively(root).toList()

        verify(exactly = 1) { fileSystem.listFiles(root, true) }
    }

    // -- helpers --

    private fun mockFileStatus(
        path: String,
        size: Long,
        mtime: Long,
    ): LocatedFileStatus =
        mockk<LocatedFileStatus> {
            every { getPath() } returns Path(path)
            every { getLen() } returns size
            every { getModificationTime() } returns mtime
        }

    private fun emptyRemoteIterator(): RemoteIterator<LocatedFileStatus> = remoteIteratorOf()

    private fun remoteIteratorOf(vararg items: LocatedFileStatus): RemoteIterator<LocatedFileStatus> {
        val iter = items.iterator()
        return object : RemoteIterator<LocatedFileStatus> {
            override fun hasNext(): Boolean = iter.hasNext()

            override fun next(): LocatedFileStatus = iter.next()
        }
    }
}
