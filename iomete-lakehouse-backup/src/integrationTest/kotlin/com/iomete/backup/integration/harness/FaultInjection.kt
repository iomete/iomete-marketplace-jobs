package com.iomete.backup.integration.harness

import com.iomete.backup.copy.TempFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.util.Progressable
import java.io.IOException
import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Config-driven fault injection for integration tests. Registered through the `hadoopOptions` seam
 * (`fs.<scheme>.impl`), each subclass wraps the target temp-file write in a stream that throws after
 * a fixed number of bytes, simulating a mid-copy failure. Failure count is keyed by a per-test id so
 * a test can fail once then recover, or fail forever.
 */
object FaultInjection {
    const val ID_KEY = "iomete.test.fault.id"
    const val AFTER_BYTES_KEY = "iomete.test.fault.afterBytes"
    const val MAX_FAILURES_KEY = "iomete.test.fault.maxFailures"
    const val TARGET_NAME_KEY = "iomete.test.fault.targetName"

    private val counters = ConcurrentHashMap<String, AtomicInteger>()

    fun s3Options(
        afterBytes: Long,
        maxFailures: Int,
        targetName: String? = null,
        id: String = UUID.randomUUID().toString(),
    ): Map<String, String> = base(afterBytes, maxFailures, targetName, id) + ("fs.s3a.impl" to FaultyS3AFileSystem::class.java.name)

    fun hdfsOptions(
        afterBytes: Long,
        maxFailures: Int,
        targetName: String? = null,
        id: String = UUID.randomUUID().toString(),
    ): Map<String, String> = base(afterBytes, maxFailures, targetName, id) + ("fs.hdfs.impl" to FaultyHdfsFileSystem::class.java.name)

    private fun base(
        afterBytes: Long,
        maxFailures: Int,
        targetName: String?,
        id: String,
    ): Map<String, String> =
        buildMap {
            put(ID_KEY, id)
            put(AFTER_BYTES_KEY, afterBytes.toString())
            put(MAX_FAILURES_KEY, maxFailures.toString())
            targetName?.let { put(TARGET_NAME_KEY, it) }
        }

    fun maybeWrap(
        conf: Configuration,
        path: Path,
        out: FSDataOutputStream,
    ): FSDataOutputStream {
        val id = conf.get(ID_KEY) ?: return out
        if (!TempFiles.isTemp(path.name)) return out

        val targetName = conf.get(TARGET_NAME_KEY)
        if (targetName != null && !path.name.endsWith("-$targetName")) return out

        val maxFailures = conf.getInt(MAX_FAILURES_KEY, -1)
        val count = counters.getOrPut(id) { AtomicInteger(0) }.incrementAndGet()
        val shouldFail = maxFailures < 0 || count <= maxFailures
        if (!shouldFail) return out

        return FSDataOutputStream(FailingOutputStream(out, conf.getLong(AFTER_BYTES_KEY, 0L)), null)
    }
}

private class FailingOutputStream(
    private val delegate: OutputStream,
    private val afterBytes: Long,
) : OutputStream() {
    private var written = 0L

    override fun write(b: Int) {
        if (written >= afterBytes) throw IOException("injected fault after $afterBytes bytes")
        delegate.write(b)
        written++
    }

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ) {
        val remaining = afterBytes - written
        if (remaining <= 0) throw IOException("injected fault after $afterBytes bytes")
        val allowed = minOf(len.toLong(), remaining).toInt()
        delegate.write(b, off, allowed)
        written += allowed
        if (allowed < len) throw IOException("injected fault after $afterBytes bytes")
    }

    override fun flush() = delegate.flush()

    override fun close() = delegate.close()
}

class FaultyS3AFileSystem : S3AFileSystem() {
    override fun create(
        f: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable?,
    ): FSDataOutputStream =
        FaultInjection.maybeWrap(conf, f, super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress))
}

class FaultyHdfsFileSystem : DistributedFileSystem() {
    override fun create(
        f: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable?,
    ): FSDataOutputStream =
        FaultInjection.maybeWrap(conf, f, super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress))
}
