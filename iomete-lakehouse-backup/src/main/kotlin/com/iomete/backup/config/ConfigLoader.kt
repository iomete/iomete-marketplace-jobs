package com.iomete.backup.config

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.iomete.backup.config.internal.Parser
import com.iomete.backup.config.internal.Utils
import com.iomete.backup.config.internal.ValidationResult
import com.iomete.backup.config.internal.Validator
import org.slf4j.LoggerFactory

object ConfigLoader {
    private val logger = LoggerFactory.getLogger(ConfigLoader::class.java)
    private val writer = jacksonObjectMapper().writerWithDefaultPrettyPrinter()

    fun load(path: String): ApplicationConfig {
        val config = Parser.parseFromFile(path)

        when (val result = Validator.validate(config)) {
            is ValidationResult.Invalid -> throw ConfigValidationException(result.errors)
            ValidationResult.Valid -> Unit
        }

        logger.info(
            "Configuration loaded (secrets redacted):\n{}",
            writer.writeValueAsString(Utils.redactSecrets(config)),
        )
        return config
    }
}
