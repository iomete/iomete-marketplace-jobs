package com.iomete.backup.copy

import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.URI
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("integration")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SameBucketDualMinioIntegrationTest {

    companion object {
        private const val bucketName = "shared-bucket"
        private const val sourceKey = "warehouse/in/file.txt"
        private const val targetKey = "warehouse/out/file.txt"
        private const val fileContent = "copied-through-s3a"

        private val minioImage = DockerImageName.parse("minio/minio:RELEASE.2023-09-04T19-57-37Z")

        @Container
        @JvmField
        val sourceMinio: MinIOContainer = MinIOContainer(minioImage)
            .withUserName("sourceuser")
            .withPassword("sourcepass123")
            .waitingFor(Wait.forHttp("/minio/health/ready"))

        @Container
        @JvmField
        val targetMinio: MinIOContainer = MinIOContainer(minioImage)
            .withUserName("targetuser")
            .withPassword("targetpass123")
            .waitingFor(Wait.forHttp("/minio/health/ready"))
    }

    private lateinit var sourceClient: S3Client
    private lateinit var targetClient: S3Client

    @BeforeEach
    fun setUpStorage() {
        sourceClient = s3Client(sourceMinio)
        targetClient = s3Client(targetMinio)

        ensureBucketExists(sourceClient)
        ensureBucketExists(targetClient)

        sourceClient.putObject(
            PutObjectRequest.builder()
                .bucket(bucketName)
                .key(sourceKey)
                .build(),
            RequestBody.fromString(fileContent)
        )
    }

    @Test
    fun `copies between same bucket names on different S3 endpoints`() {
        val sourceConfig = S3Config(
            bucket = bucketName,
            prefix = "warehouse/in",
            endpoint = sourceMinio.getS3URL(),
            pathStyleAccess = true,
            accessKey = sourceMinio.getUserName(),
            secretKey = sourceMinio.getPassword()
        )
        val targetConfig = S3Config(
            bucket = bucketName,
            prefix = "warehouse/out",
            endpoint = targetMinio.getS3URL(),
            pathStyleAccess = true,
            accessKey = targetMinio.getUserName(),
            secretKey = targetMinio.getPassword()
        )

        val copier = FileCopier(
            sourceConfMap = HadoopConfigBuilder.buildConfigMap(sourceConfig),
            targetConfMap = HadoopConfigBuilder.buildConfigMap(targetConfig),
            sourceRoot = PathResolver.resolveRootUri(sourceConfig),
            targetRoot = PathResolver.resolveRootUri(targetConfig)
        )

        val result = copier.copySingleFile("s3a://$bucketName/$sourceKey")

        assertTrue(result.success, "Copy should succeed across isolated S3 endpoints")
        assertEquals("s3a://$bucketName/$targetKey", result.targetPath)
        assertEquals(
            fileContent,
            targetClient.getObjectAsBytes(
                GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(targetKey)
                    .build()
            ).asUtf8String()
        )
    }

    private fun ensureBucketExists(client: S3Client) {
        runCatching {
            client.headBucket(
                HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build()
            )
        }.getOrElse {
            client.createBucket(
                CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build()
            )
        }
    }

    private fun s3Client(container: MinIOContainer): S3Client {
        val credentials = AwsBasicCredentials.create(container.getUserName(), container.getPassword())

        return S3Client.builder()
            .endpointOverride(URI.create(container.getS3URL()))
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .region(Region.US_EAST_1)
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build()
            )
            .build()
    }
}
