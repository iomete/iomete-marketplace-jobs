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
import java.io.File
import java.net.URI
import java.util.UUID

/**
 * Suite-wide singletons: one MinIO container, one MiniDFSCluster, one local SparkSession.
 * Started lazily on first access and shared by every integration test.
 */
object IntegrationHarness {
    private const val REGION = "us-east-1"

    private val minioLazy =
        lazy { MinIOContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z").also { it.start() } }
    val minio: MinIOContainer by minioLazy

    private val hdfsLazy =
        lazy {
            val baseDir = File("build/test/minidfs-${UUID.randomUUID()}").absolutePath
            val conf = Configuration()
            conf.set("hdfs.minidfs.basedir", baseDir)
            MiniDFSCluster
                .Builder(conf)
                .numDataNodes(1)
                .build()
                .also { it.waitClusterUp() }
        }
    val hdfs: MiniDFSCluster by hdfsLazy

    private val sparkLazy =
        lazy {
            SparkSession
                .builder()
                .appName("backup-integration")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .orCreate
        }
    val spark: SparkSession by sparkLazy

    private val s3Lazy =
        lazy {
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
    val s3: S3Client by s3Lazy

    // No hdfs.shutdown(): calling it from a JVM hook races Hadoop's own ShutdownHookManager.
    init {
        Runtime.getRuntime().addShutdownHook(
            Thread {
                if (s3Lazy.isInitialized()) runCatching { s3.close() }
                if (sparkLazy.isInitialized()) runCatching { spark.stop() }
                if (minioLazy.isInitialized()) runCatching { minio.stop() }
            },
        )
    }

    fun s3Config(
        bucket: String,
        hadoopOptions: Map<String, String> = emptyMap(),
    ): S3Config =
        S3Config(
            bucket = bucket,
            endpoint = minio.s3URL,
            pathStyleAccess = true,
            accessKey = minio.userName,
            secretKey = minio.password,
            region = REGION,
            hadoopOptions = hadoopOptions,
        )

    fun hdfsConfig(
        path: String,
        hadoopOptions: Map<String, String> = emptyMap(),
    ): HdfsConfig {
        val namenode = "localhost:${hdfs.nameNodePort}"
        return HdfsConfig(
            namenode = namenode,
            path = path,
            user = System.getProperty("user.name"),
            hadoopOptions = hadoopOptions,
        )
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
