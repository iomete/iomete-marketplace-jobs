package com.iomete.backup.copy

import com.iomete.backup.config.AuthConfig
import com.iomete.backup.config.HaConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class PathResolverTest {

    // ---- resolveRootUri ----

    @Nested
    inner class ResolveRootUri {

        @Test
        fun `S3 config with prefix produces s3a URI`() {
            val config = S3Config(
                bucket = "my-bucket",
                prefix = "data/warehouse/",
                accessKey = "key",
                secretKey = "secret"
            )
            assertEquals("s3a://my-bucket/data/warehouse", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `S3 config without prefix produces bucket-only URI`() {
            val config = S3Config(
                bucket = "my-bucket",
                prefix = "",
                accessKey = "key",
                secretKey = "secret"
            )
            assertEquals("s3a://my-bucket", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `S3 config trims leading and trailing slashes from prefix`() {
            val config = S3Config(
                bucket = "my-bucket",
                prefix = "/data/warehouse/",
                accessKey = "key",
                secretKey = "secret"
            )
            assertEquals("s3a://my-bucket/data/warehouse", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `HDFS config with namenode produces full URI`() {
            val config = HdfsConfig(
                path = "/data/warehouse",
                namenode = "hdfs://namenode:8020"
            )
            assertEquals("hdfs://namenode:8020/data/warehouse", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `HDFS config trims trailing slash from path`() {
            val config = HdfsConfig(
                path = "/data/warehouse/",
                namenode = "hdfs://namenode:8020"
            )
            assertEquals("hdfs://namenode:8020/data/warehouse", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `HDFS config with HA produces nameservice-based URI`() {
            val config = HdfsConfig(
                path = "/backups/warehouse",
                ha = HaConfig(
                    nameservice = "mycluster",
                    namenodes = listOf("nn1", "nn2"),
                    rpcAddresses = mapOf(
                        "nn1" to "namenode1:8020",
                        "nn2" to "namenode2:8020"
                    )
                )
            )
            assertEquals("hdfs://mycluster/backups/warehouse", PathResolver.resolveRootUri(config))
        }

        @Test
        fun `HDFS config with neither namenode nor HA returns path only`() {
            val config = HdfsConfig(path = "/data/warehouse")
            assertEquals("/data/warehouse", PathResolver.resolveRootUri(config))
        }
    }

    // ---- resolveTargetPath ----

    @Nested
    inner class ResolveTargetPath {

        @Test
        fun `S3 to S3 path translation preserves directory structure`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://src-bucket/data/warehouse/db/table/file.parquet",
                sourceRoot = "s3a://src-bucket/data/warehouse",
                targetRoot = "s3a://bkp-bucket/backups/warehouse"
            )
            assertEquals("s3a://bkp-bucket/backups/warehouse/db/table/file.parquet", result)
        }

        @Test
        fun `S3 to HDFS path translation`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://src-bucket/data/warehouse/db/table/part-0001.parquet",
                sourceRoot = "s3a://src-bucket/data/warehouse",
                targetRoot = "hdfs://namenode:8020/backups/warehouse"
            )
            assertEquals("hdfs://namenode:8020/backups/warehouse/db/table/part-0001.parquet", result)
        }

        @Test
        fun `HDFS to S3 path translation`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "hdfs://namenode:8020/data/warehouse/db/table/file.parquet",
                sourceRoot = "hdfs://namenode:8020/data/warehouse",
                targetRoot = "s3a://bkp-bucket/backups"
            )
            assertEquals("s3a://bkp-bucket/backups/db/table/file.parquet", result)
        }

        @Test
        fun `handles trailing slashes on source root`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://bucket/prefix/dir/file.txt",
                sourceRoot = "s3a://bucket/prefix/",
                targetRoot = "s3a://target/out"
            )
            assertEquals("s3a://target/out/dir/file.txt", result)
        }

        @Test
        fun `handles trailing slashes on target root`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://bucket/prefix/file.txt",
                sourceRoot = "s3a://bucket/prefix",
                targetRoot = "s3a://target/out/"
            )
            assertEquals("s3a://target/out/file.txt", result)
        }

        @Test
        fun `deeply nested file preserves full relative path`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://bucket/root/a/b/c/d/e/file.parquet",
                sourceRoot = "s3a://bucket/root",
                targetRoot = "s3a://backup/dest"
            )
            assertEquals("s3a://backup/dest/a/b/c/d/e/file.parquet", result)
        }

        @Test
        fun `file directly under source root`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://bucket/root/file.parquet",
                sourceRoot = "s3a://bucket/root",
                targetRoot = "s3a://backup/dest"
            )
            assertEquals("s3a://backup/dest/file.parquet", result)
        }

        @Test
        fun `fallback when source path does not start with source root`() {
            val result = PathResolver.resolveTargetPath(
                sourceFilePath = "s3a://other-bucket/somewhere/file.csv",
                sourceRoot = "s3a://bucket/root",
                targetRoot = "s3a://backup/dest"
            )
            // Falls back to using just the file name
            assertEquals("s3a://backup/dest/file.csv", result)
        }
    }
}
