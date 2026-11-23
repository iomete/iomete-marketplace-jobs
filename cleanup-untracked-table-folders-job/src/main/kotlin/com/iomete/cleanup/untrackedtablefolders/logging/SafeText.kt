package com.iomete.cleanup.untrackedtablefolders.logging

import java.util.Collections
import java.util.IdentityHashMap

// Every text the job writes to a log, an audit value or a report goes through here. Credential-like content is
// redacted, control, format and separator characters are written as \uXXXX so a name cannot start a new log line,
// reorder one or hide a character, and the length is bounded. Code that reads exception messages to classify an
// error keeps the raw text.
object SafeText {
    const val MAX_LENGTH = 8_192
    const val MAX_ERROR_LENGTH = 2_000
    const val MAX_AUDIT_ERROR_LENGTH = 1_000
    const val REDACTED = "***"
    const val TRUNCATION_MARKER = "...[truncated]"

    private const val MAX_PART_LENGTH = 1_000
    private const val MAX_CAUSE_NAMES_LENGTH = 300
    private const val MAX_CAUSE_DEPTH = 10
    private const val MAX_FRAMES_PER_THROWABLE = 40
    private const val MAX_SUPPRESSED = 5
    private const val MAX_TRACE_LINES = 400

    // Redaction reads at most this much, so a huge message, such as one that embeds a query plan, costs bounded time.
    private const val MAX_SCANNED_LENGTH = 65_536

    fun text(
        value: String?,
        maxLength: Int = MAX_LENGTH,
    ): String = bound(escape(redact(scanned(value.toString())), escapeBackslash = false), maxLength)

    // A storage path keeps its name, so only credentials in the URI authority are removed. Folder names such as
    // token=1 are data here, unlike in free text. A backslash is written as \\, so the text reads back to the exact
    // name. Two different names give different text unless a URL user or password is removed or the text is cut.
    fun path(
        value: String?,
        maxLength: Int = MAX_LENGTH,
    ): String = bound(escape(redactUserInfo(scanned(value.toString())), escapeBackslash = true), maxLength)

    // The last pass over a value that path or text may already have made safe. A backslash is kept, so such a value
    // comes back unchanged, and anything still unsafe is escaped.
    fun safetyNet(
        value: String?,
        maxLength: Int = MAX_LENGTH,
    ): String = bound(escape(redactUserInfo(scanned(value.toString())), escapeBackslash = false), maxLength)

    // For audit rows, which more people can read than the job log: the top-level message and the class of each
    // cause. Cause messages can hold connection details, so they stay in the log.
    fun errorMessage(
        error: Throwable,
        maxLength: Int = MAX_AUDIT_ERROR_LENGTH,
        withClassName: Boolean = false,
    ): String {
        val chain = causeChain(error)
        val causeNames =
            bound(
                text(
                    chain.drop(1).joinToString("") { "; caused by ${it.javaClass.name}" } +
                        if (chain.isCut) "; caused by ..." else "",
                ),
                MAX_CAUSE_NAMES_LENGTH,
            )
        val top = if (withClassName) headline(error) else messageOf(error) ?: error.javaClass.name
        val message = text(top, (maxLength - causeNames.length).coerceAtLeast(MIN_LENGTH))
        return bound(message + causeNames, maxLength)
    }

    // One line for WARN and ERROR: the class and message of the error and of each cause.
    fun describe(
        error: Throwable,
        maxLength: Int = MAX_ERROR_LENGTH,
    ): String {
        val chain = causeChain(error)
        val parts = chain.map { text(headline(it), MAX_PART_LENGTH) } + listOfNotNull("...".takeIf { chain.isCut })
        return bound(parts.joinToString("; caused by "), maxLength)
    }

    // The stack trace for DEBUG, one safe line per entry. Frames, causes, suppressed errors and lines are bounded.
    fun trace(error: Throwable): List<String> {
        val lines = mutableListOf<String>()
        val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())

        fun add(line: String): Boolean {
            if (lines.size >= MAX_TRACE_LINES) {
                return false
            }
            lines += line
            return true
        }

        fun render(
            throwable: Throwable,
            label: String,
            indent: String,
            depth: Int,
        ) {
            if (!seen.add(throwable)) {
                add("$indent${label}[circular reference: ${text(throwable.javaClass.name)}]")
                return
            }
            if (depth > MAX_CAUSE_DEPTH) {
                add("$indent$label...")
                return
            }
            if (!add(indent + label + text(headline(throwable), MAX_PART_LENGTH))) {
                return
            }
            val frames = runCatching { throwable.stackTrace }.getOrNull().orEmpty()
            // Frames hold class, method and file names only, so they are escaped but not redacted.
            frames.take(MAX_FRAMES_PER_THROWABLE).forEach {
                add("$indent    at ${bound(escape(it.toString(), escapeBackslash = false), MAX_PART_LENGTH)}")
            }
            if (frames.size > MAX_FRAMES_PER_THROWABLE) {
                add("$indent    ... ${frames.size - MAX_FRAMES_PER_THROWABLE} more")
            }
            val suppressed = runCatching { throwable.suppressed }.getOrNull().orEmpty()
            suppressed.take(MAX_SUPPRESSED).forEach { render(it, "Suppressed: ", "$indent    ", depth + 1) }
            if (suppressed.size > MAX_SUPPRESSED) {
                add("$indent    ... ${suppressed.size - MAX_SUPPRESSED} more suppressed")
            }
            causeOf(throwable)?.let { render(it, "Caused by: ", indent, depth + 1) }
        }

        render(error, "", "", 0)
        if (lines.size >= MAX_TRACE_LINES) {
            lines[lines.lastIndex] = "... trace truncated"
        }
        return lines
    }

    private fun headline(throwable: Throwable): String =
        messageOf(throwable)?.let { "${throwable.javaClass.name}: $it" } ?: throwable.javaClass.name

    // A broken exception must not hide the one being reported.
    private fun messageOf(throwable: Throwable): String? = runCatching { throwable.message }.getOrNull()

    private fun causeOf(throwable: Throwable): Throwable? = runCatching { throwable.cause }.getOrNull()

    private class CauseChain(
        throwables: List<Throwable>,
        val isCut: Boolean,
    ) : List<Throwable> by throwables

    private fun causeChain(error: Throwable): CauseChain {
        val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
        val chain = mutableListOf<Throwable>()
        var current: Throwable? = error
        while (current != null && chain.size < MAX_CAUSE_DEPTH && seen.add(current)) {
            chain += current
            current = causeOf(current)
        }
        return CauseChain(chain, isCut = current != null)
    }

    // A cut token is dropped, because part of a secret may match no rule.
    private fun scanned(value: String): String {
        if (value.length <= MAX_SCANNED_LENGTH) {
            return value
        }
        val head = value.substring(0, MAX_SCANNED_LENGTH)
        return head.substring(0, head.indexOfLast { it.isWhitespace() }.coerceAtLeast(0)) + TRUNCATION_MARKER
    }

    private fun bound(
        value: String,
        maxLength: Int,
    ): String {
        require(maxLength >= MIN_LENGTH) { "maxLength must be at least $MIN_LENGTH" }
        if (value.length <= maxLength) {
            return value
        }
        var end = maxLength - TRUNCATION_MARKER.length
        // Keep a surrogate pair and an escape sequence whole.
        if (value[end - 1].isHighSurrogate()) {
            end -= 1
        }
        val escapeStart = value.lastIndexOf("\\u", end - 1)
        if (escapeStart >= 0 && escapeStart + ESCAPE_LENGTH > end) {
            end = escapeStart
        }
        // A cut between the two backslashes of \\ would leave one that starts no escape.
        var backslashes = 0
        while (backslashes < end && value[end - 1 - backslashes] == '\\') {
            backslashes += 1
        }
        if (backslashes % 2 == 1) {
            end -= 1
        }
        return value.substring(0, end) + TRUNCATION_MARKER
    }

    private fun escape(
        value: String,
        escapeBackslash: Boolean,
    ): String =
        buildString(value.length + 16) {
            var index = 0
            while (index < value.length) {
                val codePoint = value.codePointAt(index)
                val end = index + Character.charCount(codePoint)
                when {
                    escapeBackslash && codePoint == '\\'.code -> append("\\\\")
                    isUnsafe(codePoint) -> (index until end).forEach { append("\\u%04X".format(value[it].code)) }
                    else -> append(value, index, end)
                }
                index = end
            }
        }

    // Control, format, line and paragraph separator characters and unpaired surrogates, the set the audit SQL text
    // escapes. Format characters include the bidi controls, which can reorder how a line is shown, and invisible ones
    // such as U+200B, which make two different names look the same. The Tags block is taken whole, because its
    // unassigned code points are invisible too.
    private fun isUnsafe(codePoint: Int): Boolean =
        when (Character.getType(codePoint).toByte()) {
            Character.CONTROL, Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR, Character.SURROGATE -> true
            else -> codePoint in TAGS
        }

    private fun redact(value: String): String = REDACTIONS.fold(value) { text, redaction -> redaction(text) }

    private fun redactUserInfo(value: String): String =
        USER_INFO.replace(value) { match ->
            val scheme = match.groupValues[1]
            // abfs://container@account names a container, not a user.
            if (match.groups[2] != null && scheme.removeSuffix("://").lowercase() in AZURE_SCHEMES) {
                match.value
            } else {
                "$scheme$REDACTED@"
            }
        }

    private const val MIN_LENGTH = 32
    private const val ESCAPE_LENGTH = 6

    private val AZURE_SCHEMES = setOf("abfs", "abfss", "wasb", "wasbs")

    private val TAGS = 0xE0000..0xE007F

    private const val KEYWORDS =
        "secret[._\\s-]?key|access[._\\s-]?key|account[._\\s-]?key|private[._\\s-]?key|api[._\\s-]?key|" +
            "secret|password|passwd|pwd|token|credential|session|signature|authorization|cookie"

    // An exception class such as ExpiredTokenException, or a credentials provider, is a name and not a key.
    private val NAME_ENDINGS = listOf("exception", "error", "throwable", "provider", "providers")

    // A key match starts at the keyword, so the name before it can have any length and stays outside the match. The
    // part after the keyword is bounded and possessive, so a long run of name characters cannot make the match slow;
    // it stops before the "-" of "->". The lookbehinds stop a name ending without consuming the text after it.
    private val KEY = "(?:$KEYWORDS)(?:[A-Za-z0-9_.]|-(?!>)){0,128}+" + NAME_ENDINGS.joinToString("") { "(?<!$it)" }

    private val KEYWORD = Regex(KEYWORDS, RegexOption.IGNORE_CASE)

    private const val VALUE =
        "\\\\\"(?:(?!\\\\\")[\\s\\S])*+(?:\\\\\"|\\z)" +
            "|\"(?:[^\"\\\\]|\\\\[\\s\\S])*+(?:\"|\\z)" +
            "|'(?:[^'\\\\]|\\\\[\\s\\S])*+(?:'|\\z)" +
            "|[^\\s,;&\"'(){}\\[\\]<>]++"

    // Each part of a URI is read up to a fixed length, so text with many words or many "://" stays linear. Longer
    // user info or a longer path before the query is not a URI this job sees; key=value still covers its query.
    private const val SCHEME = "[A-Za-z][A-Za-z0-9+.-]{0,31}://"

    private const val UP_TO_LAST_AT = "(?:[^\\s@]{0,1024}+@)++"

    // A password can hold an unencoded @, so a user and password run to the last @ before a space. A user alone
    // (group 2) has neither : nor @.
    private val USER_INFO = Regex("\\b($SCHEME)(?:[^\\s/@:]{0,1024}+:$UP_TO_LAST_AT|([^\\s/@:]{1,1024}+)@)")

    private val REDACTIONS: List<(String) -> String> =
        listOf(
            rule("-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\\z)", RegexOption.DOT_MATCHES_ALL) {
                "-----BEGIN PRIVATE KEY-----$REDACTED"
            },
            // A JSON Web Token.
            rule("\\beyJ[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]*") { REDACTED },
            // AWS access key ids: long-term, temporary, STS bearer and context credentials.
            rule("(?<![A-Z0-9])(AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}(?![A-Z0-9])") { "${it.groupValues[1]}$REDACTED" },
            ::redactUserInfo,
            // jdbc:oracle:thin:user/password@host
            rule("\\b(jdbc:oracle:[A-Za-z0-9]{1,16}:)[^\\s/@:]{1,1024}+/$UP_TO_LAST_AT", RegexOption.IGNORE_CASE) {
                "${it.groupValues[1]}$REDACTED@"
            },
            // The whole query, which carries presigned-URL signatures and tokens.
            rule("\\b($SCHEME[^\\s?#\"'<>]{0,2048}+)\\?[^\\s#\"'<>]++") { "${it.groupValues[1]}?$REDACTED" },
            // An Azure SAS signature outside a URL, such as a SAS token set as a value, whose sig= follows an &.
            rule("(?<![A-Za-z0-9_.-])(sig=)[^\\s&\"'<>]++", RegexOption.IGNORE_CASE) { "${it.groupValues[1]}$REDACTED" },
            // Before key=value, which would otherwise redact only the scheme of "Authorization: Bearer x".
            rule("\\b(bearer|basic|digest|negotiate)\\s+[^\\s\"',;]+", RegexOption.IGNORE_CASE) { "${it.groupValues[1]} $REDACTED" },
            // Hadoop configuration XML. The whole name is read, so its length does not matter.
            rule("(<name>([^<]*+)</name>\\s*+<value>)[^<]*+(</value>)", RegexOption.IGNORE_CASE) {
                if (isKeyName(it.groupValues[2].trim())) "${it.groupValues[1]}$REDACTED${it.groupValues[3]}" else it.value
            },
            // key=value, key: value, "key":"value", key -> value (Scala maps), key => value.
            rule("($KEY)(\\\\?[\"']?\\s*+(?:=>|->|=|:)\\s*+)($VALUE)", RegexOption.IGNORE_CASE) {
                it.groupValues[1] + it.groupValues[2] + maskedValue(it.groupValues[3])
            },
            // A long secret after a key with no = or :, for example "secret key wJalr...". It must hold a digit, so a
            // class name such as SimpleAWSCredentialsProvider is kept.
            rule("($KEY)([\\s\"']{1,4})(?=[A-Za-z0-9+/_=-]*\\d)[A-Za-z0-9+/_=-]{16,}+", RegexOption.IGNORE_CASE) {
                "${it.groupValues[1]}${it.groupValues[2]}$REDACTED"
            },
        )

    private fun isKeyName(name: String): Boolean =
        KEYWORD.containsMatchIn(name) && NAME_ENDINGS.none { name.endsWith(it, ignoreCase = true) }

    private fun maskedValue(value: String): String =
        when {
            value.startsWith("\\\"") -> "\\\"$REDACTED\\\""
            value.startsWith("\"") -> "\"$REDACTED\""
            value.startsWith("'") -> "'$REDACTED'"
            else -> REDACTED
        }

    private fun rule(
        pattern: String,
        vararg options: RegexOption,
        replacement: (MatchResult) -> String,
    ): (String) -> String {
        val regex = Regex(pattern, options.toSet())
        return { text -> regex.replace(text, replacement) }
    }
}
