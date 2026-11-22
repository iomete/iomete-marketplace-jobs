package com.iomete.cleanup.untrackedtablefolders.logging

import org.jboss.logging.Logger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.net.ConnectException
import java.time.Duration

private const val SECRET = "hunter2Secret9"

// Every character SafeText must never write raw.
private val UNSAFE_CHARACTERS =
    (0x00..0x1F).map(Int::toChar) + (0x7F..0x9F).map(Int::toChar) +
        listOf('\u2028', '\u2029', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E') +
        listOf('\u2066', '\u2067', '\u2068', '\u2069', '\u200E', '\u200F', '\u061C') +
        listOf('\u200B', '\uFEFF', '\u2060', '\u00AD', '\u180E', '\uFFF9', '\uFFFA', '\uFFFB')

// Reads SafeText.path back: \\ is one backslash and \uXXXX one UTF-16 unit. Any other backslash fails the read.
private fun decodePath(text: String): String =
    buildString {
        var index = 0
        while (index < text.length) {
            if (text[index] != '\\') {
                append(text[index])
                index += 1
                continue
            }
            when (text.getOrNull(index + 1)) {
                '\\' -> append('\\')
                'u' -> {
                    val hex = text.substring(index + 2, (index + 6).coerceAtMost(text.length))
                    require(hex.length == 4 && hex.all { it in '0'..'9' || it in 'A'..'F' }) { "a cut escape at $index in $text" }
                    append(hex.toInt(16).toChar())
                    index += 4
                }
                else -> throw IllegalArgumentException("a backslash that starts no escape at $index in $text")
            }
            index += 2
        }
    }

class SafeTextTest {

    @Test
    fun `credentials in a URL authority are removed but an Azure container is kept`() {
        assertEquals("s3a://***@bucket/db", SafeText.text("s3a://AKIAIOSFODNN7EXAMPLE:$SECRET/with/slash@bucket/db"))
        assertEquals("https://***@host/x", SafeText.text("https://user:$SECRET@host/x"))
        assertEquals("https://***@github.com/org/repo", SafeText.text("https://ghp${SECRET}@github.com/org/repo"))
        assertEquals("abfss://***@acct.dfs.core.windows.net/x", SafeText.text("abfss://container:$SECRET@acct.dfs.core.windows.net/x"))

        val azure = "abfss://container@acct.dfs.core.windows.net/lake/sales.db"
        assertEquals(azure, SafeText.text(azure))
        assertEquals(azure, SafeText.path(azure))
        // An @ after the first slash is part of the path.
        assertEquals("s3a://bucket/db/folder@2024", SafeText.text("s3a://bucket/db/folder@2024"))
    }

    @Test
    fun `a URL query string is removed whole`() {
        val presigned =
            "GET https://minio:9000/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20261120" +
                "&X-Amz-Security-Token=$SECRET&X-Amz-Signature=0f1e2d3c4b5a done"

        assertEquals("GET https://minio:9000/bucket/key?*** done", SafeText.text(presigned))
    }

    @Test
    fun `an Azure SAS signature is removed in every form, with or without a URL`() {
        assertEquals("fs.azure.sas.fixed.token=***&sig=***", SafeText.text("fs.azure.sas.fixed.token=sv=2020-02-10&sig=FAKESIGCANARY1"))
        assertEquals("sv=2020-02-10&ss=b&sig=***&se=2026", SafeText.text("sv=2020-02-10&ss=b&sig=FAKESIG%2BCANARY2%3D&se=2026"))
        assertEquals("sig=***", SafeText.text("sig=FAKESIGCANARY3"))
        assertEquals("container sas: sp=r&sig=*** end", SafeText.text("container sas: sp=r&sig=FAKESIGCANARY4 end"))
        assertEquals("https://acct.blob.core.windows.net/c?***", SafeText.text("https://acct.blob.core.windows.net/c?sv=1&sig=FAKESIGCANARY5"))
        // A name that only ends in sig is kept.
        assertEquals("config=1 xsig=2 a.sig=3", SafeText.text("config=1 xsig=2 a.sig=3"))
    }

    @Test
    fun `a password with an at sign and an Oracle user and password are removed whole`() {
        assertEquals("s3a://***@bucket/path", SafeText.text("s3a://user:pa@ss$SECRET@bucket/path"))
        assertEquals("s3a://***@bucket/path", SafeText.path("s3a://user:pa@ss$SECRET@bucket/path"))
        assertEquals("s3a://***@bucket/db", SafeText.text("s3a://AKIAIOSFODNN7EXAMPLE:p@ss/x$SECRET@bucket/db"))

        assertEquals("jdbc:oracle:thin:***@db.example.com:1521/ORCL", SafeText.text("jdbc:oracle:thin:scott/$SECRET@db.example.com:1521/ORCL"))
        assertEquals("JDBC:ORACLE:THIN:***@//db:1521/svc", SafeText.text("JDBC:ORACLE:THIN:scott/\"p@ss$SECRET\"@//db:1521/svc"))
        assertEquals("url=jdbc:oracle:oci:***@db", SafeText.text("url=jdbc:oracle:oci:scott/$SECRET@db"))

        // No user and password, so nothing is removed.
        listOf("jdbc:oracle:thin:@db.example.com:1521:ORCL", "jdbc:oracle:thin:@//db:1521/svc", "s3a://bucket/db/folder@2024")
            .forEach { assertEquals(it, SafeText.text(it)) }
        assertEquals("s3a://bucket/db/folder@2024", SafeText.path("s3a://bucket/db/folder@2024"))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "secret", "client_secret", "password", "db.passwd", "PWD", "token", "fs.s3a.session.token", "credential",
            "rest.auth.oauth2.credential", "access_key", "fs.s3a.access.key", "accessKey", "s3.access-key-id",
            "secret-key", "fs.s3a.secret.key", "s3.secret-access-key", "session", "JSESSIONID", "signature",
            "X-Amz-Signature", "authorization", "private_key", "fs.gs.auth.service.account.private.key", "api-key",
            "fs.azure.account.key.acct.dfs.core.windows.net", "Cookie",
        ],
    )
    fun `the value of every credential-like key is redacted in each notation`(key: String) {
        assertEquals("$key=***, next=1", SafeText.text("$key=$SECRET, next=1"))
        assertEquals("$key: *** next", SafeText.text("$key: $SECRET next"))
        assertEquals("{\"$key\":\"***\",\"next\":1}", SafeText.text("{\"$key\":\"$SECRET\",\"next\":1}"))
        assertEquals("{\\\"$key\\\": \\\"***\\\"}", SafeText.text("{\\\"$key\\\": \\\"$SECRET\\\"}"))
        assertEquals("Map($key -> ***, path -> s3a://b/x)", SafeText.text("Map($key -> $SECRET, path -> s3a://b/x)"))
        assertEquals("$key => '***'", SafeText.text("$key => '$SECRET with spaces'"))
        assertEquals("<name>$key</name><value>***</value>", SafeText.text("<name>$key</name><value>$SECRET</value>"))
    }

    @Test
    fun `a key with a long name before the keyword is still redacted in each notation`() {
        val bucketKey = "fs.s3a.bucket." + "b".repeat(130) + ".secret.key"
        val catalogKey = "spark.sql.catalog." + "c".repeat(200) + ".s3.secret-access-key"
        val prefixed = "x".repeat(130) + "_password"

        listOf("token", "fs.s3a.secret-key", bucketKey, catalogKey, prefixed, "k".repeat(5_000) + "token").forEach { key ->
            assertEquals("$key=***, next=1", SafeText.text("$key=$SECRET, next=1"))
            assertEquals("$key: *** next", SafeText.text("$key: $SECRET next"))
            assertEquals("{\"$key\":\"***\",\"next\":1}", SafeText.text("{\"$key\":\"$SECRET\",\"next\":1}"))
            assertEquals("Map($key -> ***, path -> s3a://b/x)", SafeText.text("Map($key -> $SECRET, path -> s3a://b/x)"))
            assertEquals("Map($key->***,path->s3a://b/x)", SafeText.text("Map($key->$SECRET,path->s3a://b/x)"))
            assertEquals("<name>$key</name><value>***</value>", SafeText.text("<name>$key</name><value>$SECRET</value>"))
            assertEquals("<name> $key </name>\\u000A  <value>***</value>", SafeText.text("<name> $key </name>\n  <value>$SECRET</value>"))
            assertEquals("$key ***", SafeText.text("$key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
        }

        // A long name is still only a name when it holds no keyword or ends like a class name.
        listOf(
            "x".repeat(200) + ".endpoint=http://minio:9000",
            "x".repeat(200) + ".aws.credentials.provider=org.example.SimpleAWSCredentialsProvider",
            "<name>" + "x".repeat(200) + ".endpoint</name><value>http://minio:9000</value>",
            "<name>fs.s3a.aws.credentials.provider</name><value>org.example.SimpleAWSCredentialsProvider</value>",
            "<name>" + "x".repeat(200) + ".credentials.provider</name><value>org.example.SimpleAWSCredentialsProvider</value>",
        ).forEach { assertEquals(it, SafeText.text(it)) }
    }

    @Test
    fun `redaction of a 100,000 character hostile input finishes well under a second`() {
        // The scan reads up to the last space before 64 KiB, so each input puts its hostile part in front of a space.
        // Many URI starts in a row cost more in the older URI rules and are timed in the test above.
        fun hostile(part: String) = (part.repeat(65_000 / part.length + 1).take(65_000) + " ").padEnd(100_000, 'y')

        val longKey = hostile("x".repeat(64_000) + ".secret.key=$SECRET")
        val inputs =
            listOf(
                longKey,
                hostile("token"),
                hostile("secret.token.password"),
                hostile("password" + "_".repeat(127)),
                hostile("tokenerror" + "-".repeat(126)),
                hostile("token-"),
                hostile("token\\\":"),
                hostile("secret key "),
                hostile("a.b-c_"),
                hostile("<name>"),
                hostile("<name>token"),
                hostile("s3a://a:b@"),
                hostile("s3a://a:" + "b".repeat(64_000)),
                hostile("jdbc:oracle:thin:a/"),
            )

        inputs.forEach { input ->
            assertEquals(100_000, input.length)
            assertTimeoutPreemptively(Duration.ofMillis(1_000)) { SafeText.text(input) }
        }
        assertFalse(SafeText.text(longKey, maxLength = 200_000).contains(SECRET))
    }

    @Test
    fun `an authorization scheme token is redacted before the header value`() {
        assertEquals("Authorization: *** ***", SafeText.text("Authorization: Bearer $SECRET"))
        assertEquals("sent Bearer *** to the catalog", SafeText.text("sent Bearer $SECRET to the catalog"))
        assertEquals("Basic ***", SafeText.text("Basic dXNlcjpwYXNzd29yZA=="))
    }

    @Test
    fun `AWS access key ids are redacted wherever they appear`() {
        assertEquals("key AKIA*** and ASIA*** and ABIA*** and ACCA***", SafeText.text("key AKIAIOSFODNN7EXAMPLE and ASIA1234567890ABCDEF and ABIA1234567890ABCDEF and ACCA1234567890ABCDEF"))
        // 15 characters after the prefix, or part of a longer word, is not a key id.
        assertEquals("AKIA123456789012345", SafeText.text("AKIA123456789012345"))
        assertEquals("XAKIAIOSFODNN7EXAMPLE", SafeText.text("XAKIAIOSFODNN7EXAMPLE"))
    }

    @Test
    fun `a long secret after a key without a separator is redacted, but a class name is kept`() {
        assertEquals("secret key ***", SafeText.text("secret key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
        assertEquals("token \"***\"", SafeText.text("token \"0123456789abcdefABCDEF\""))

        val s3aMessage = "No AWS Credentials provided by TemporaryAWSCredentialsProvider SimpleAWSCredentialsProvider"
        assertEquals(s3aMessage, SafeText.text(s3aMessage))
    }

    @Test
    fun `JSON web tokens and private key blocks are redacted anywhere`() {
        assertEquals(
            "got *** back",
            SafeText.text("got eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U back"), // gitleaks:allow
        )
        assertEquals(
            "key: -----BEGIN PRIVATE KEY-----***", // gitleaks:allow
            SafeText.text("key: -----BEGIN RSA PRIVATE KEY-----\nMIIEow$SECRET\n-----END RSA PRIVATE KEY-----"),
        )
    }

    @Test
    fun `names that only contain a keyword are kept`() {
        listOf(
            "fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", // gitleaks:allow
            "com.amazonaws.services.securitytoken.model.ExpiredTokenException: The security token included in the request is expired",
            "Deleted untracked table folder: path=s3a://bucket/db/orphan",
            "Cleanup failed for catalog=spark_catalog, database=sales",
            "Creating SparkSession for cleanup untracked table folders job",
        ).forEach { assertEquals(it, SafeText.text(it)) }

        // The text after a kept name is still read.
        assertEquals("x.ExpiredTokenException: token=***", SafeText.text("x.ExpiredTokenException: token=$SECRET"))
    }

    @Test
    fun `a path keeps folder names that look like key=value but loses URI credentials`() {
        assertEquals("s3a://bucket/db/token=1/session=2", SafeText.path("s3a://bucket/db/token=1/session=2"))
        assertEquals("s3a://***@bucket/db", SafeText.path("s3a://key:$SECRET@bucket/db"))
        assertEquals("s3a://bucket/db/a\\u000Ab", SafeText.path("s3a://bucket/db/a\nb"))
    }

    @Test
    fun `a path reads back exactly, so two different names give different text when no URL credential is removed`() {
        // A backslash is doubled, so a folder literally named a\u000Ab and a folder with a newline stay apart.
        assertEquals("s3a://bucket/db/a\\\\u000Ab", SafeText.path("s3a://bucket/db/a\\u000Ab"))
        assertEquals("s3a://bucket/db/a\\u000Ab", SafeText.path("s3a://bucket/db/a\nb"))
        assertEquals("s3a://bucket/db/c\\\\d", SafeText.path("s3a://bucket/db/c\\d"))

        val names =
            listOf(
                "a\nb", "a\\u000Ab", "a\\nb", "a\\", "\\", "\\\\", "a\\\\u000Ab", "\\u", "x\u200By", "x\\u200By", "t\uDB40\uDC41g",
                "t\\uDB40\\uDC41g", "lone\uD800", "lone\\uD800", "token=1", "caf\u00E9", "\uD83D\uDE00", "\u202Egpj.exe",
            )
        val written = names.map { SafeText.path("s3a://bucket/db/$it") }
        assertEquals(names.size, written.toSet().size, written.toString())
        names.zip(written).forEach { (name, text) -> assertEquals("s3a://bucket/db/$name", decodePath(text), text) }

        // A path with no backslash and nothing to escape is written as it is.
        listOf("s3a://bucket/db/orders", "s3a://bucket/db/token=1/session=2", "abfss://c@acct.dfs.core.windows.net/x", "gs://b/caf\u00E9 x")
            .forEach { assertEquals(it, SafeText.path(it)) }
    }

    @Test
    fun `a cut path ends on a whole escape, so what is kept still reads back`() {
        val name = "s3a://bucket/db/" + "\\\n\\u0041\uD83D\uDE00\u200Bz\\\\".repeat(30)

        (32..300).forEach { maxLength ->
            val written = SafeText.path(name, maxLength)
            assertTrue(written.length <= maxLength)
            assertTrue(written.endsWith(SafeText.TRUNCATION_MARKER))
            val kept = decodePath(written.removeSuffix(SafeText.TRUNCATION_MARKER))
            assertTrue(name.startsWith(kept), "maxLength=$maxLength: $written")
        }
    }

    @Test
    fun `a log line hides a path part that looks like a credential, while the audit path keeps it`() {
        capturingLogs { records ->
            SafeLogger.getLogger(SafeTextTest::class.java).warn("Deleted untracked table folder: path=s3a://bucket/db/token=1")

            assertEquals("Deleted untracked table folder: path=s3a://bucket/db/token=***", records.single().message)
        }
        assertEquals("s3a://bucket/db/token=1", SafeText.path("s3a://bucket/db/token=1"))
    }

    @Test
    fun `control, separator and bidi characters are written as escapes`() {
        UNSAFE_CHARACTERS.forEach { char ->
            val escaped = SafeText.text("a${char}b")
            assertEquals("a\\u%04Xb".format(char.code), escaped)
            assertEquals(escaped, SafeText.path("a${char}b"))
        }

        val forged = SafeText.text("s3a://bucket/db/orphan\r\n26/11/20 10:00:00 WARN App: forged\u202Egpj.exe")
        assertEquals(1, forged.lines().size)
        assertTrue(forged.none { it in UNSAFE_CHARACTERS })
        assertEquals("caf\u00E9 \u65E5\u672C", SafeText.text("caf\u00E9 \u65E5\u672C"))
    }

    @Test
    fun `every invisible format character is written as an escape, so two names that look the same stay apart`() {
        val listed = listOf(0x200B, 0xFEFF, 0x2060, 0x00AD, 0x180E, 0xFFF9, 0xFFFA, 0xFFFB, 0xE0000, 0xE0001, 0xE0041, 0xE007F)
        val format = (0..Character.MAX_CODE_POINT).filter { Character.getType(it) == Character.FORMAT.toInt() }
        val tags = (0xE0000..0xE007F).toList()
        assertTrue(format.size > 150, "format characters known to this JDK: ${format.size}")

        (listed + format + tags).distinct().forEach { codePoint ->
            val char = String(Character.toChars(codePoint))
            val escaped = "a" + char.map { "\\u%04X".format(it.code) }.joinToString("") + "b"
            assertEquals(escaped, SafeText.text("a${char}b"), "U+%04X".format(codePoint))
            assertEquals(escaped, SafeText.path("a${char}b"), "U+%04X".format(codePoint))
        }
        assertEquals("a\\uD800b\\uDC00", SafeText.text("a\uD800b\uDC00"))
        assertEquals("a\\uDC00\\uD800b", SafeText.path("a\uDC00\uD800b"))
        assertFalse(SafeText.text("s3a://bucket/db/orders") == SafeText.text("s3a://bucket/db/orders\u200B"))

        // Visible characters stay as they are, an emoji with its variation selector included.
        val visible = "caf\u00E9 \u65E5\u672C \uD83D\uDE00 \u2764\uFE0F \u05D0\u05D1 \uD834\uDD1E"
        assertEquals(visible, SafeText.text(visible))
        assertEquals(visible, SafeText.path(visible))
    }

    @Test
    fun `text is bounded with a marker, without splitting a surrogate pair or an escape`() {
        val bounded = SafeText.text("a".repeat(5_000), maxLength = 100)
        assertEquals(100, bounded.length)
        assertTrue(bounded.endsWith(SafeText.TRUNCATION_MARKER))
        assertTrue(SafeText.text("a".repeat(20_000)).length <= SafeText.MAX_LENGTH)

        val emoji = "\uD83D\uDE00"
        val withPair = SafeText.text("a".repeat(85) + emoji.repeat(10), maxLength = 100)
        assertFalse(withPair.removeSuffix(SafeText.TRUNCATION_MARKER).last().isHighSurrogate())

        val withEscape = SafeText.text("a".repeat(84) + "\u001B".repeat(10), maxLength = 100)
        assertTrue(withEscape.removeSuffix(SafeText.TRUNCATION_MARKER).matches(Regex("a{84}(\\\\u001B)*")))
    }

    @Test
    fun `a very long input is scanned only in part, and a token cut at the scan limit is dropped`() {
        // The scan limit falls inside the password, before the @, so the URL rule cannot match what is left of it.
        val value = "x ".repeat(32_755) + "https://user:CANARY0123456789abcdef@host/" + "y".repeat(10_000)

        val safe = SafeText.text(value, maxLength = 200_000)

        assertFalse(safe.contains("CANARY"))
        assertTrue(safe.endsWith(SafeText.TRUNCATION_MARKER))
    }

    @Test
    fun `redaction time stays bounded on hostile input`() {
        // Each input is under the scan limit, so every rule runs over all of it.
        assertTimeoutPreemptively(Duration.ofSeconds(10)) {
            SafeText.text("token".repeat(13_000))
            SafeText.text("a.".repeat(32_000))
            SafeText.text("secret.token.password".repeat(3_000) + " x")
            SafeText.text("password=\"".repeat(6_000))
            SafeText.text("password=\\\"".repeat(5_000))
            SafeText.text("s3a://" + "a:".repeat(32_000))
            SafeText.text("https://" + "a@".repeat(32_000))
            SafeText.text("token ".repeat(10_000) + "=")
            SafeText.text("<name>token</name><value>".repeat(2_500))
            SafeText.text("-----BEGIN PRIVATE KEY-----".repeat(2_000))
            SafeText.text("x://".repeat(16_000))
            SafeText.text("x://a?".repeat(10_000))
            SafeText.text("secret ".repeat(1) + "1".repeat(60_000))
        }
    }

    @Test
    fun `sanitizing twice gives the same text`() {
        listOf(
            "fs.s3a.secret.key=$SECRET and Authorization: Bearer $SECRET",
            "s3a://k:$SECRET@bucket/x?sig=1",
            "a\u001Bb\nc",
            "{\"password\":\"$SECRET\"}",
            "a".repeat(10_000),
        ).forEach { value ->
            val once = SafeText.text(value)
            assertEquals(once, SafeText.text(once))
        }
    }

    @Test
    fun `describe walks the cause chain with safe, bounded messages`() {
        val error =
            IllegalStateException(
                "top password=$SECRET",
                IOException("inner\ntoken=$SECRET", ConnectException("refused")),
            )

        assertEquals(
            "java.lang.IllegalStateException: top password=***; caused by java.io.IOException: inner\\u000Atoken=***; " +
                "caused by java.net.ConnectException: refused",
            SafeText.describe(error),
        )
        assertTrue(SafeText.describe(IllegalStateException("x".repeat(50_000))).length <= SafeText.MAX_ERROR_LENGTH)
    }

    @Test
    fun `the audit message keeps the top message and only the class of each cause`() {
        val error = IllegalStateException("Failed to list folders: password=$SECRET", IOException("host minio.internal:9000 refused"))

        assertEquals("Failed to list folders: password=***; caused by java.io.IOException", SafeText.errorMessage(error))
        assertEquals(
            "java.lang.IllegalStateException: Failed to list folders: password=***; caused by java.io.IOException",
            SafeText.errorMessage(error, withClassName = true),
        )
        assertEquals("java.lang.IllegalStateException", SafeText.errorMessage(IllegalStateException()))
        assertTrue(SafeText.errorMessage(IllegalStateException("x".repeat(50_000), IOException())).endsWith("; caused by java.io.IOException"))
        assertTrue(SafeText.errorMessage(IllegalStateException("x".repeat(50_000))).length <= SafeText.MAX_AUDIT_ERROR_LENGTH)
    }

    @Test
    fun `a cause cycle and a deep chain end`() {
        val first = Exception("first")
        val second = Exception("second", first)
        first.initCause(second)

        assertEquals("java.lang.Exception: first; caused by java.lang.Exception: second; caused by ...", SafeText.describe(first))
        assertTrue(SafeText.trace(first).any { it.contains("[circular reference: java.lang.Exception]") })
        assertEquals("first; caused by java.lang.Exception; caused by ...", SafeText.errorMessage(first))

        val deep =
            (1..50).fold(Exception("root")) { cause, index -> Exception("level $index", cause) }
                .also { generateSequence<Throwable>(it) { error -> error.cause }.forEach { error -> error.stackTrace = emptyArray() } }
        assertEquals(11, SafeText.describe(deep).split("; caused by ").size)
        assertTrue(SafeText.trace(deep).last().trim() == "Caused by: ...")
    }

    @Test
    fun `a message that throws does not hide the error`() {
        val broken =
            object : RuntimeException() {
                override val message: String get() = throw IllegalStateException("no message")
            }

        assertEquals(broken.javaClass.name, SafeText.describe(broken))
    }

    @Test
    fun `the trace escapes frames without redacting them and is bounded`() {
        val error = IllegalStateException("boom\u001B[2J password=$SECRET", IOException("cause"))
        error.stackTrace = arrayOf(StackTraceElement("org.apache.spark.sql.SparkSession", "sql", "SparkSession.scala", 123))
        error.addSuppressed(IOException("suppressed token=$SECRET"))

        val trace = SafeText.trace(error)

        assertEquals("java.lang.IllegalStateException: boom\\u001B[2J password=***", trace.first())
        assertTrue("    at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:123)" in trace)
        assertTrue(trace.any { it.startsWith("    Suppressed: java.io.IOException: suppressed token=***") })
        assertTrue(trace.any { it.startsWith("Caused by: java.io.IOException: cause") })
        assertTrue(trace.none { line -> line.any { it in UNSAFE_CHARACTERS } || line.contains(SECRET) })

        val wide = Exception("wide").apply { repeat(500) { addSuppressed(Exception("s$it")) } }
        assertTrue(SafeText.trace(wide).size <= 400)
        val tall = Exception("tall").apply { stackTrace = Array(500) { StackTraceElement("C", "m$it", "C.kt", it) } }
        assertTrue(SafeText.trace(tall).last().trim() == "... 460 more")
    }

    @Test
    fun `the logger hands the backend only safe text and never a Throwable`() {
        capturingLogs { records ->
            val logger = SafeLogger.getLogger(SafeTextTest::class.java)

            logger.warn("Could not list root=s3a://bucket/db\nFORGED", IOException("password=$SECRET"))
            logger.debug("details", IOException("token=$SECRET"))
            logger.infoLines(listOf("REPORT", "- s3a://bucket/db/a\nb", "end"))

            assertTrue(records.all { it.thrown == null })
            assertTrue(records.none { it.message.contains(SECRET) })

            val warn = records.single { it.level == Logger.Level.WARN }
            assertEquals("Could not list root=s3a://bucket/db\\u000AFORGED (java.io.IOException: password=***)", warn.message)

            val debug = records.filter { it.level == Logger.Level.DEBUG }
            assertEquals(2, debug.size)
            assertTrue(debug[0].message.startsWith("Could not list root=s3a://bucket/db\\u000AFORGED\njava.io.IOException: password=***\n    at "))
            assertTrue(debug[1].message.startsWith("details\njava.io.IOException: token=***\n    at "))

            val report = records.single { it.level == Logger.Level.INFO }
            assertEquals("REPORT\n- s3a://bucket/db/a\\u000Ab\nend", report.message)
        }
    }

    @Test
    fun `the logger skips the trace when DEBUG is off`() {
        val records = mutableListOf<CapturedLog>()
        val quiet =
            object : Logger("quiet") {
                override fun isEnabled(level: Level?): Boolean = level != Level.DEBUG && level != Level.TRACE

                override fun doLog(level: Level, loggerClassName: String?, message: Any?, parameters: Array<out Any?>?, thrown: Throwable?) {
                    records += CapturedLog(level, name, message.toString(), thrown)
                }

                override fun doLogf(level: Level, loggerClassName: String?, format: String, parameters: Array<out Any?>?, thrown: Throwable?) {
                    records += CapturedLog(level, name, format, thrown)
                }
            }
        io.mockk.mockkStatic(Logger::class)
        try {
            io.mockk.every { Logger.getLogger(SafeTextTest::class.java) } returns quiet
            SafeLogger.getLogger(SafeTextTest::class.java).error("failed", IOException("x"))
        } finally {
            io.mockk.unmockkStatic(Logger::class)
        }

        assertEquals(listOf(Logger.Level.ERROR), records.map { it.level })
        assertNull(records.single().thrown)
    }
}
