package com.iomete.backup.copy

import com.iomete.backup.copy.internal.PathResolver
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PathResolverTest {
    @Nested
    inner class ResolveTargetPath {
        @Test
        fun `S3 to S3 path translation preserves directory structure`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://src-bucket/data/warehouse/db/table/file.parquet",
                    sourceRoot = "s3a://src-bucket/data/warehouse",
                    targetRoot = "s3a://bkp-bucket/backups/warehouse",
                )
            assertEquals("s3a://bkp-bucket/backups/warehouse/db/table/file.parquet", result)
        }

        @Test
        fun `S3 to HDFS path translation`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://src-bucket/data/warehouse/db/table/part-0001.parquet",
                    sourceRoot = "s3a://src-bucket/data/warehouse",
                    targetRoot = "hdfs://namenode:8020/backups/warehouse",
                )
            assertEquals("hdfs://namenode:8020/backups/warehouse/db/table/part-0001.parquet", result)
        }

        @Test
        fun `HDFS to S3 path translation`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "hdfs://namenode:8020/data/warehouse/db/table/file.parquet",
                    sourceRoot = "hdfs://namenode:8020/data/warehouse",
                    targetRoot = "s3a://bkp-bucket/backups",
                )
            assertEquals("s3a://bkp-bucket/backups/db/table/file.parquet", result)
        }

        @Test
        fun `handles trailing slashes on source root`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://bucket/prefix/dir/file.txt",
                    sourceRoot = "s3a://bucket/prefix/",
                    targetRoot = "s3a://target/out",
                )
            assertEquals("s3a://target/out/dir/file.txt", result)
        }

        @Test
        fun `handles trailing slashes on target root`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://bucket/prefix/file.txt",
                    sourceRoot = "s3a://bucket/prefix",
                    targetRoot = "s3a://target/out/",
                )
            assertEquals("s3a://target/out/file.txt", result)
        }

        @Test
        fun `deeply nested file preserves full relative path`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://bucket/root/a/b/c/d/e/file.parquet",
                    sourceRoot = "s3a://bucket/root",
                    targetRoot = "s3a://backup/dest",
                )
            assertEquals("s3a://backup/dest/a/b/c/d/e/file.parquet", result)
        }

        @Test
        fun `file path equal to source root maps to target root`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://bucket/root",
                    sourceRoot = "s3a://bucket/root",
                    targetRoot = "s3a://backup/dest",
                )
            assertEquals("s3a://backup/dest", result)
        }

        @Test
        fun `file directly under source root`() {
            val result =
                PathResolver.resolveTargetPath(
                    sourceFilePath = "s3a://bucket/root/file.parquet",
                    sourceRoot = "s3a://bucket/root",
                    targetRoot = "s3a://backup/dest",
                )
            assertEquals("s3a://backup/dest/file.parquet", result)
        }

        @Test
        fun `rejects source path outside source root`() {
            val error =
                assertFailsWith<IllegalArgumentException> {
                    PathResolver.resolveTargetPath(
                        sourceFilePath = "s3a://other-bucket/somewhere/file.csv",
                        sourceRoot = "s3a://bucket/root",
                        targetRoot = "s3a://backup/dest",
                    )
                }

            assertEquals(
                "Source file path 's3a://other-bucket/somewhere/file.csv' is not under source root 's3a://bucket/root'",
                error.message,
            )
        }
    }
}
