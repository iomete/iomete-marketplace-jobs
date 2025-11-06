package com.iomete.spark.udf

import org.apache.hadoop.hive.ql.exec.UDF

import java.io.InputStream
import java.net.{HttpURLConnection, URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.io.Source

class HttpRequest extends UDF {
  import HttpRequest._

  def evaluate(url: String): String =
    fetch(url, Map.empty, Map.empty)

  def evaluate(url: String, params: java.util.Map[String, String]): String =
    fetch(url, toScalaMap(params), Map.empty)

  def evaluate(
      url: String,
      params: java.util.Map[String, String],
      headers: java.util.Map[String, String]
  ): String =
    fetch(url, toScalaMap(params), toScalaMap(headers))
}

object HttpRequest {

  private val CharsetName = StandardCharsets.UTF_8.name()
  private val DefaultTimeoutMillis = 5000

  private def readStream(stream: InputStream): String = {
    val source = Source.fromInputStream(stream, CharsetName)
    try source.mkString
    finally source.close()
  }

  private def encode(value: String): String =
    URLEncoder.encode(value, CharsetName)

  private def filterMap(entries: Map[String, String]): Map[String, String] =
    Option(entries)
      .getOrElse(Map.empty[String, String])
      .collect { case (k, v) if k != null => k -> Option(v).getOrElse("") }

  private def buildUrl(baseUrl: String, params: Map[String, String]): String = {
    val effectiveParams = filterMap(params)
    val encodedPairs = effectiveParams.map {
      case (k, v) =>
        val encodedKey = encode(k)
        val encodedValue = encode(v)
        s"$encodedKey=$encodedValue"
    }

    if (encodedPairs.isEmpty) {
      baseUrl
    } else {
      val newQuery = encodedPairs.mkString("&")
      try {
        val uri = new URI(baseUrl)
        val combinedQuery = Option(uri.getRawQuery) match {
          case Some(existing) if existing.nonEmpty => s"$existing&$newQuery"
          case Some(_)                             => newQuery
          case None                                => newQuery
        }

        new URI(
          uri.getScheme,
          uri.getUserInfo,
          uri.getHost,
          uri.getPort,
          uri.getPath,
          combinedQuery,
          uri.getFragment
        ).toString
      } catch {
        case _: Exception =>
          val separator = if (baseUrl.contains("?")) "&" else "?"
          s"$baseUrl$separator$newQuery"
      }
    }
  }

  private[iomete] def toScalaMap(map: java.util.Map[String, String]): Map[String, String] =
    Option(map).map(_.asScala.toMap).getOrElse(Map.empty[String, String])

  private[iomete] def fetch(
      url: String,
      params: Map[String, String],
      headers: Map[String, String]
  ): String = {
    if (url == null || url.trim.isEmpty) {
      null
    } else {
      val finalUrl = buildUrl(url, params)
      val connection = new URL(finalUrl).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(DefaultTimeoutMillis)
      connection.setReadTimeout(DefaultTimeoutMillis)

      filterMap(headers).foreach {
        case (key, value) =>
          connection.setRequestProperty(key, value)
      }

      try {
        val status = connection.getResponseCode
        val stream =
          if (status >= 200 && status < 300) {
            connection.getInputStream
          } else {
            val errorStream = connection.getErrorStream
            if (errorStream != null) errorStream else connection.getInputStream
          }

        readStream(stream)
      } finally {
        connection.disconnect()
      }
    }
  }

  def apply(
      url: String,
      params: Map[String, String] = Map.empty,
      headers: Map[String, String] = Map.empty
  ): String =
    fetch(url, Option(params).getOrElse(Map.empty[String, String]), Option(headers).getOrElse(Map.empty[String, String]))

  def apply(
      url: String,
      params: java.util.Map[String, String],
      headers: java.util.Map[String, String]
  ): String =
    fetch(url, toScalaMap(params), toScalaMap(headers))
}
