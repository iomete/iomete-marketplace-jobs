package com.iomete.backup.integration.harness

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SparkSession
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI
import java.util.UUID

/**
 * Suite-wide singletons: one MinIO container, one MiniDFSCluster, one local SparkSession.
 * Started lazily on first access and shared by every integration test.
 */
object IntegrationHarness {
    private const val REGION = "us-east-1"

    val minio: MinIOContainer by lazy {
        MinIOContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z").also { it.start() }
    }

    val hdfs: MiniDFSCluster by lazy {
        val baseDir = java.io.File("build/test/minidfs-${UUID.randomUUID()}").absolutePath
        val conf = Configuration()
        conf.set("hdfs.minidfs.basedir", baseDir)
        MiniDFSCluster
            .Builder(conf)
            .numDataNodes(1)
            .build()
            .also { it.waitClusterUp() }
    }

    val spark: SparkSession by lazy {
        SparkSession
            .builder()
            .appName("backup-integration")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .orCreate
    }

    val s3: S3Client by lazy {
        S3Client
            .builder()
            .endpointOverride(URI(minio.s3URL))
            .region(Region.of(REGION))
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(minio.userName, minio.password),
                ),
            ).build()
    }

    fun s3Config(bucket: String): S3Config =
        S3Config(
            bucket = bucket,
            endpoint = minio.s3URL,
            pathStyleAccess = true,
            accessKey = minio.userName,
            secretKey = minio.password,
            region = REGION,
        )

    fun hdfsConfig(path: String): HdfsConfig {
        val namenode = "localhost:${hdfs.nameNodePort}"
        return HdfsConfig(namenode = namenode, path = path, user = System.getProperty("user.name"))
    }

    /** A fresh, empty bucket for one test. */
    fun freshBucket(): String {
        val bucket = "it-${UUID.randomUUID()}"
        s3.createBucket { it.bucket(bucket) }
        return bucket
    }

    /** A fresh HDFS target path for one test (not created; the job creates parents). */
    fun freshHdfsPath(): String = "backup-it/${UUID.randomUUID()}"
}
