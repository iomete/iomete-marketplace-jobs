package com.iomete.cleanup.untrackedtablefolders.catalog

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.RuntimeConfig
import scala.Predef
import scala.collection.JavaConverters

// getAll() reads the map at call time, as the real session config reads SQLConf.
internal fun sessionConfigOf(settings: Map<String, String>): RuntimeConfig =
    mockk<RuntimeConfig>().also { config ->
        every { config.getAll() } answers { JavaConverters.mapAsScalaMapConverter(settings).asScala().toMap(Predef.`$conforms`()) }
    }
