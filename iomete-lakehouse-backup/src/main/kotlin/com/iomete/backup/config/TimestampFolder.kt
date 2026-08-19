package com.iomete.backup.config

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.IsoFields

object TimestampFolder {
    private val FORMATTERS =
        mapOf(
            "hourly" to DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"),
            "daily" to DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            "weekly" to
                DateTimeFormatterBuilder()
                    .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
                    .appendLiteral("-W")
                    .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
                    .toFormatter(),
            "monthly" to DateTimeFormatter.ofPattern("yyyy-MM"),
        ).mapValues { it.value.withZone(ZoneOffset.UTC) }

    val supported: Set<String> get() = FORMATTERS.keys

    fun folderName(
        granularity: String,
        at: Instant,
    ): String =
        requireNotNull(FORMATTERS[granularity]) {
            "Unsupported timestamp folder granularity '$granularity'"
        }.format(at)
}
