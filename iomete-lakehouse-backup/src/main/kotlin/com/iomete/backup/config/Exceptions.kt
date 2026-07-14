package com.iomete.backup.config

open class ConfigException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

class ConfigParseException(
    message: String,
    cause: Throwable? = null,
) : ConfigException(message, cause)

class ConfigValidationException(
    val errors: List<String>,
) : ConfigException("Configuration is invalid:\n" + errors.joinToString("\n") { "  - $it" })
