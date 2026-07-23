package com.iomete.backup.integration.fixtures

import com.iomete.backup.integration.harness.IntegrationHarness
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import kotlin.random.Random
import kotlin.test.assertEquals

/**
 * Deterministic seed tree exercising path-resolution edge cases. The returned map doubles as the
 * expected target state, so verification is byte-for-byte against exactly what was seeded.
 */
fun fixture(): Map<String, ByteArray> =
    linkedMapOf(
        "root.txt" to "root file".toByteArray(),
        "data/warehouse/table/metadata/v1.metadata.json" to "{\"format\":1}".toByteArray(),
        "data/warehouse/table/data/part-00000.parquet" to "PAR1..payload..PAR1".toByteArray(),
        "empty.txt" to ByteArray(0),
        "with space/file name.txt" to "spaced".toByteArray(),
        "unicode/файл-数据-δ.txt" to "unicode".toByteArray(),
        "big/random.bin" to Random(42).nextBytes(5 * 1024 * 1024),
    )

/** Write a key -> bytes map into an S3 bucket. */
fun seedS3(
    bucket: String,
    tree: Map<String, ByteArray>,
) {
    tree.forEach { (key, bytes) ->
        IntegrationHarness.s3.putObject(
            { it.bucket(bucket).key(key) },
            RequestBody.fromBytes(bytes),
        )
    }
}

/** Read every object in a bucket back as key -> bytes. */
fun readS3(bucket: String): Map<String, ByteArray> {
    val result = linkedMapOf<String, ByteArray>()
    var token: String? = null

    do {
        val resp =
            IntegrationHarness.s3.listObjectsV2(
                ListObjectsV2Request
                    .builder()
                    .bucket(bucket)
                    .continuationToken(token)
                    .build(),
            )
        resp.contents().filterNot { it.key().endsWith("/") }.forEach { obj ->
            result[obj.key()] =
                IntegrationHarness.s3.getObjectAsBytes { it.bucket(bucket).key(obj.key()) }.asByteArray()
        }
        token = if (resp.isTruncated) resp.nextContinuationToken() else null
    } while (token != null)

    return result
}

/** Read every file under an HDFS path back as key -> bytes, keyed relative to the path. */
fun readHdfs(path: String): Map<String, ByteArray> {
    val fs = IntegrationHarness.hdfs.fileSystem
    val root = Path("hdfs://localhost:${IntegrationHarness.hdfs.nameNodePort}/$path")

    if (!fs.exists(root)) return emptyMap()

    val result = linkedMapOf<String, ByteArray>()
    val rootStr = root.toUri().path.trimEnd('/')
    val it = fs.listFiles(root, true)

    while (it.hasNext()) {
        val status = it.next()
        val key =
            status.path
                .toUri()
                .path
                .removePrefix("$rootStr/")
        result[key] = readAll(fs, status.path)
    }

    return result
}

private fun readAll(
    fs: FileSystem,
    path: Path,
): ByteArray = fs.open(path).use { it.readBytes() }

/** Assert exact key set and byte-for-byte content. Both missing and extra files fail. */
fun assertMatches(
    expected: Map<String, ByteArray>,
    actual: Map<String, ByteArray>,
) {
    assertEquals(expected.keys, actual.keys, "target key set mismatch")

    expected.forEach { (key, bytes) ->
        assertEquals(bytes.toList(), actual.getValue(key).toList(), "content mismatch for $key")
    }
}
