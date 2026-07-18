package com.iomete.backup.config

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class HdfsConfigTest {
    @Test
    fun `HDFS config with path produces hdfs URI`() {
        val config =
            HdfsConfig(
                namenode = "isilon.example.com:8020",
                path = "backups/warehouse",
                user = "isilon-user",
            )
        assertEquals("hdfs://isilon.example.com:8020/backups/warehouse", config.rootUri)
    }

    @Test
    fun `HDFS config without path produces namenode-only URI`() {
        val config =
            HdfsConfig(
                namenode = "isilon.example.com:8020",
                path = "",
                user = "isilon-user",
            )
        assertEquals("hdfs://isilon.example.com:8020", config.rootUri)
    }

    @Test
    fun `HDFS config trims leading and trailing slashes from path`() {
        val config =
            HdfsConfig(
                namenode = "isilon.example.com:8020",
                path = "/backups/warehouse/",
                user = "isilon-user",
            )
        assertEquals("hdfs://isilon.example.com:8020/backups/warehouse", config.rootUri)
    }

    @Test
    fun `HDFS config carries the configured user`() {
        val config =
            HdfsConfig(
                namenode = "isilon.example.com:8020",
                user = "isilon-user",
            )
        assertEquals("isilon-user", config.user)
    }
}
