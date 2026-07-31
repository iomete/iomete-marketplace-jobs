package com.iomete.backup.fs

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI

fun <T> useFileSystem(
    config: StorageConfig,
    root: String,
    block: (FileSystem) -> T,
): T = FileSystemFactory.create(config, URI(root), HadoopConfigBuilder.build(config)).use(block)

object FileSystemFactory {
    // HDFS simple auth carries identity as the username (3-arg factory);
    // S3 uses the credentials already in conf.
    fun create(
        config: StorageConfig,
        uri: URI,
        conf: Configuration,
    ): FileSystem =
        when (config) {
            is S3Config -> FileSystem.newInstance(uri, conf)
            is HdfsConfig -> FileSystem.newInstance(uri, conf, config.user)
        }
}
