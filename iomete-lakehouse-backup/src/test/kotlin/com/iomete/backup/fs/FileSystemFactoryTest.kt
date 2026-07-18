package com.iomete.backup.fs

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.Test
import java.net.URI
import kotlin.test.assertSame

class FileSystemFactoryTest {
    @Test
    fun `S3 config builds a filesystem without binding a user`() {
        val uri = URI("s3a://bucket/path")
        val conf = Configuration()
        val fs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(uri, conf) } returns fs

            val result =
                FileSystemFactory.create(
                    S3Config(bucket = "bucket", accessKey = "k", secretKey = "s"),
                    uri,
                    conf,
                )

            assertSame(fs, result)
            verify(exactly = 1) { FileSystem.newInstance(uri, conf) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `HDFS config builds a filesystem bound to the configured user`() {
        val uri = URI("hdfs://isilon.example.com:8020/path")
        val conf = Configuration()
        val fs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(uri, conf, "isilon-user") } returns fs

            val result =
                FileSystemFactory.create(
                    HdfsConfig(namenode = "isilon.example.com:8020", user = "isilon-user"),
                    uri,
                    conf,
                )

            assertSame(fs, result)
            verify(exactly = 1) { FileSystem.newInstance(uri, conf, "isilon-user") }
            verify(exactly = 0) { FileSystem.newInstance(uri, conf) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }
}
