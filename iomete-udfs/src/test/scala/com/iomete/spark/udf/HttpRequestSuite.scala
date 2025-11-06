package com.iomete.spark.udf

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.OutputStream
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.JavaConverters._

class HttpRequestSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val Charset = StandardCharsets.UTF_8
  private var server: HttpServer = _
  private var baseUri: URI = _
  private var executor: ExecutorService = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val httpServer = HttpServer.create(new InetSocketAddress(0), 0)
    httpServer.createContext(
      "/echo",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val query = Option(exchange.getRequestURI.getRawQuery).getOrElse("")
          val header = Option(exchange.getRequestHeaders.getFirst("X-Test-Header")).getOrElse("")
          val response = s"$query|$header"
          val bytes = response.getBytes(Charset)
          exchange.sendResponseHeaders(200, bytes.length)
          val os: OutputStream = exchange.getResponseBody
          try os.write(bytes)
          finally {
            os.close()
            exchange.close()
          }
        }
      }
    )
    executor = Executors.newSingleThreadExecutor()
    httpServer.setExecutor(executor)
    httpServer.start()
    server = httpServer
    baseUri = new URI(s"http://127.0.0.1:${server.getAddress.getPort}/echo")
  }

  override protected def afterAll(): Unit = {
    try {
      if (server != null) {
        server.stop(0)
      }
      if (executor != null) {
        executor.shutdownNow()
      }
    } finally {
      super.afterAll()
    }
  }

  test("apply returns body for simple GET request") {
    val response = HttpRequest.apply(baseUri.toString)
    assert(response === "|")
  }

  test("apply attaches query parameters") {
    val response = HttpRequest.apply(
      baseUri.toString,
      params = Map("foo" -> "bar baz", "empty" -> "")
    )
    assert(response.split("\\|").head === "foo=bar+baz&empty=")
  }

  test("apply attaches headers") {
    val response = HttpRequest.apply(
      baseUri.toString,
      headers = Map("X-Test-Header" -> "header-value")
    )
    assert(response === "|header-value")
  }

  test("java.util.Map overload works") {
    val response = HttpRequest.apply(
      baseUri.toString,
      params = Map("foo" -> "bar").asJava,
      headers = Map("X-Test-Header" -> "value").asJava
    )
    assert(response === "foo=bar|value")
  }

  test("null url returns null") {
    assert(HttpRequest.apply(null) === null)
  }
}
