package com.iomete.backup.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ConfigParserTest {

    @Test
    fun `parse minimal S3-to-S3 config`() {
        val json = """
        {
          "source": {
            "type": "s3",
            "bucket": "source-bucket",
            "prefix": "data/",
            "accessKey": "access123",
            "secretKey": "secret456"
          },
          "target": {
            "type": "s3",
            "bucket": "target-bucket",
            "prefix": "backup/",
            "accessKey": "access789",
            "secretKey": "secret012"
          }
        }
        """.trimIndent()

        val config = ConfigParser.parse(json)

        assertIs<S3Config>(config.source)
        assertIs<S3Config>(config.target)

        val source = config.source as S3Config
        assertEquals("source-bucket", source.bucket)
        assertEquals("data/", source.prefix)
        assertEquals("access123", source.accessKey)
        assertEquals("secret456", source.secretKey)
        assertEquals(false, source.pathStyleAccess) // default
        assertNull(source.endpoint) // optional

        val target = config.target as S3Config
        assertEquals("target-bucket", target.bucket)
        assertEquals("backup/", target.prefix)

        assertEquals(20, config.copy.options.maxMaps)
        assertEquals(3, config.copy.options.maxAttempts)
        assertEquals(1000L, config.copy.options.retryDelayMs)
    }

    @Test
    fun `parse copy retry options`() {
        val json = """
        {
          "source": {
            "type": "s3",
            "bucket": "source-bucket",
            "accessKey": "access123",
            "secretKey": "secret456"
          },
          "target": {
            "type": "s3",
            "bucket": "target-bucket",
            "accessKey": "access789",
            "secretKey": "secret012"
          },
          "copy": {
            "options": {
              "maxMaps": 10,
              "maxAttempts": 5,
              "retryDelayMs": 2500
            }
          }
        }
        """.trimIndent()

        val config = ConfigParser.parse(json)

        assertEquals(10, config.copy.options.maxMaps)
        assertEquals(5, config.copy.options.maxAttempts)
        assertEquals(2500L, config.copy.options.retryDelayMs)
    }

//    @Test
//    fun `parse S3-to-HDFS config with simple auth`() {
//        val json = """
//        {
//          "source": {
//            "type": "s3",
//            "bucket": "source-bucket",
//            "prefix": "data/",
//            "endpoint": "https://s3.example.com",
//            "pathStyleAccess": true,
//            "accessKey": "access123",
//            "secretKey": "secret456"
//          },
//          "target": {
//            "type": "hdfs",
//            "path": "/backups/warehouse",
//            "namenode": "hdfs://namenode:8020",
//            "auth": {
//              "type": "simple",
//              "user": "hadoop"
//            }
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        assertIs<S3Config>(config.source)
//        assertIs<HdfsConfig>(config.target)
//
//        val source = config.source as S3Config
//        assertEquals("https://s3.example.com", source.endpoint)
//        assertTrue(source.pathStyleAccess)
//
//        val target = config.target as HdfsConfig
//        assertEquals("/backups/warehouse", target.path)
//        assertEquals("hdfs://namenode:8020", target.namenode)
//        assertNull(target.ha)
//        assertIs<AuthConfig.Simple>(target.auth)
//        assertEquals("hadoop", (target.auth as AuthConfig.Simple).user)
//    }

//    @Test
//    fun `parse HDFS-to-S3 config`() {
//        val json = """
//        {
//          "source": {
//            "type": "hdfs",
//            "path": "/data/warehouse",
//            "namenode": "hdfs://namenode:8020",
//            "auth": {
//              "type": "simple",
//              "user": "hdfs"
//            }
//          },
//          "target": {
//            "type": "s3",
//            "bucket": "backup-bucket",
//            "prefix": "backups/",
//            "accessKey": "access123",
//            "secretKey": "secret456"
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        assertIs<HdfsConfig>(config.source)
//        assertIs<S3Config>(config.target)
//
//        val source = config.source as HdfsConfig
//        assertEquals("/data/warehouse", source.path)
//        assertEquals("hdfs://namenode:8020", source.namenode)
//    }
//
//    @Test
//    fun `parse HDFS-to-HDFS config`() {
//        val json = """
//        {
//          "source": {
//            "type": "hdfs",
//            "path": "/data/warehouse",
//            "namenode": "hdfs://source-namenode:8020"
//          },
//          "target": {
//            "type": "hdfs",
//            "path": "/backups/warehouse",
//            "namenode": "hdfs://target-namenode:8020"
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        assertIs<HdfsConfig>(config.source)
//        assertIs<HdfsConfig>(config.target)
//
//        val source = config.source as HdfsConfig
//        assertEquals("hdfs://source-namenode:8020", source.namenode)
//
//        val target = config.target as HdfsConfig
//        assertEquals("hdfs://target-namenode:8020", target.namenode)
//    }
//
//    @Test
//    fun `parse HDFS config with HA`() {
//        val json = """
//        {
//          "source": {
//            "type": "s3",
//            "bucket": "source-bucket",
//            "accessKey": "key",
//            "secretKey": "secret"
//          },
//          "target": {
//            "type": "hdfs",
//            "path": "/backups/warehouse",
//            "ha": {
//              "nameservice": "mycluster",
//              "namenodes": ["nn1", "nn2"],
//              "rpcAddresses": {
//                "nn1": "namenode1.example.com:8020",
//                "nn2": "namenode2.example.com:8020"
//              }
//            },
//            "auth": {
//              "type": "simple",
//              "user": "hdfs"
//            }
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        val target = config.target as HdfsConfig
//        assertNull(target.namenode)
//
//        val ha = target.ha!!
//        assertEquals("mycluster", ha.nameservice)
//        assertEquals(listOf("nn1", "nn2"), ha.namenodes)
//        assertEquals("namenode1.example.com:8020", ha.rpcAddresses["nn1"])
//        assertEquals("namenode2.example.com:8020", ha.rpcAddresses["nn2"])
//    }
//
//    @Test
//    fun `parse HDFS config with Kerberos auth`() {
//        val json = """
//        {
//          "source": {
//            "type": "s3",
//            "bucket": "source-bucket",
//            "accessKey": "key",
//            "secretKey": "secret"
//          },
//          "target": {
//            "type": "hdfs",
//            "path": "/backups/warehouse",
//            "namenode": "hdfs://namenode:8020",
//            "auth": {
//              "type": "kerberos",
//              "principal": "hdfs-backup@EXAMPLE.COM",
//              "keytabPath": "/etc/security/keytabs/hdfs-backup.keytab"
//            }
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        val target = config.target as HdfsConfig
//        assertIs<AuthConfig.Kerberos>(target.auth)
//
//        val auth = target.auth as AuthConfig.Kerberos
//        assertEquals("hdfs-backup@EXAMPLE.COM", auth.principal)
//        assertEquals("/etc/security/keytabs/hdfs-backup.keytab", auth.keytabPath)
//    }
//
//    @Test
//    fun `handle missing optional fields with defaults`() {
//        val json = """
//        {
//          "source": {
//            "type": "hdfs",
//            "path": "/data/warehouse",
//            "namenode": "hdfs://namenode:8020"
//          },
//          "target": {
//            "type": "s3",
//            "bucket": "backup-bucket",
//            "accessKey": "key",
//            "secretKey": "secret"
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        // HDFS auth defaults to simple with "hdfs" user
//        val source = config.source as HdfsConfig
//        assertIs<AuthConfig.Simple>(source.auth)
//        assertEquals("hdfs", (source.auth as AuthConfig.Simple).user)
//
//        // S3 prefix defaults to empty
//        val target = config.target as S3Config
//        assertEquals("", target.prefix)
//
//        // Copy config defaults
//        assertEquals(CopyMode.FULL, config.copy.mode)
//        assertEquals(IncrementalStrategy.MTIME, config.copy.incrementalStrategy)
//        assertEquals(false, config.copy.options.skipCrcCheck)
//        assertEquals(false, config.copy.options.ignoreFailures)
//        assertEquals(20, config.copy.options.maxMaps)
//        assertNull(config.copy.options.bandwidthMb)
//        assertEquals(1, config.copy.options.numListStatusThreads)
//    }

//    @Test
//    fun `parse full copy config with all options`() {
//        val json = """
//        {
//          "source": {
//            "type": "s3",
//            "bucket": "source-bucket",
//            "accessKey": "key",
//            "secretKey": "secret"
//          },
//          "target": {
//            "type": "s3",
//            "bucket": "target-bucket",
//            "accessKey": "key2",
//            "secretKey": "secret2"
//          },
//          "copy": {
//            "mode": "incremental",
//            "incrementalStrategy": "checksum",
//            "options": {
//              "skipCrcCheck": true,
//              "ignoreFailures": true,
//              "maxMaps": 150,
//              "bandwidthMb": 1024,
//              "numListStatusThreads": 30
//            }
//          }
//        }
//        """.trimIndent()
//
//        val config = ConfigParser.parse(json)
//
//        assertEquals(CopyMode.INCREMENTAL, config.copy.mode)
//        assertEquals(IncrementalStrategy.CHECKSUM, config.copy.incrementalStrategy)
//        assertEquals(true, config.copy.options.skipCrcCheck)
//        assertEquals(true, config.copy.options.ignoreFailures)
//        assertEquals(150, config.copy.options.maxMaps)
//        assertEquals(1024, config.copy.options.bandwidthMb)
//        assertEquals(30, config.copy.options.numListStatusThreads)
//    }

    @Test
    fun `reject malformed JSON`() {
        val malformedJson = """
        {
          "source": {
            "type": "s3"
            "bucket": "missing-comma"
          }
        }
        """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(malformedJson)
        }
    }

    @Test
    fun `reject config with missing required source`() {
        val json = """
        {
          "target": {
            "type": "s3",
            "bucket": "target-bucket",
            "accessKey": "key",
            "secretKey": "secret"
          }
        }
        """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(json)
        }
    }

    @Test
    fun `reject config with unknown storage type`() {
        val json = """
        {
          "source": {
            "type": "gcs",
            "bucket": "source-bucket"
          },
          "target": {
            "type": "s3",
            "bucket": "target-bucket",
            "accessKey": "key",
            "secretKey": "secret"
          }
        }
        """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(json)
        }
    }
}
