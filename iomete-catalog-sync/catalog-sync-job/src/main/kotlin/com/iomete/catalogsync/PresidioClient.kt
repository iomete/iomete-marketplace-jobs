package com.iomete.catalogsync

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient
import jakarta.inject.Singleton
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path

@Singleton
@RegisterRestClient(configKey = "presidio")
interface PresidioClient {
    @POST
    @Path("/analyze")
    fun analyze(data: PresidioRequest): List<PresidioResponse>
}

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class PresidioRequest(
    val text: String = "",
    val language: String = "en",
    val scoreThreshold: Float = 0.6f
)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class PresidioResponse(
    val analysisExplanation: String? = null,
    val entityType: EntityType? = null,
    val score: Float = 0f,
    val start: Int = 0,
    val end: Int = 0,
)

val PCI_ENTITY_TYPES = setOf(
    EntityType.IBAN_CODE,
    EntityType.CREDIT_CARD,
    EntityType.US_BANK_NUMBER
)

val PII_ENTITY_TYPES = setOf(
    EntityType.PERSON,
    EntityType.PHONE_NUMBER,
    EntityType.US_DRIVER_LICENSE,
    EntityType.EMAIL_ADDRESS,
    EntityType.US_SSN,
    EntityType.US_PASSPORT,
    EntityType.LOCATION,
    EntityType.UK_NHS,
    EntityType.US_ITIN,
    EntityType.SG_NRIC_FIN,
)


enum class EntityType {
    PHONE_NUMBER,
    US_ITIN,
    SG_NRIC_FIN,
    IBAN_CODE,
    US_DRIVER_LICENSE,
    CREDIT_CARD,
    DOMAIN_NAME,
    IP_ADDRESS,
    NRP,
    LOCATION,
    EMAIL_ADDRESS,
    DATE_TIME,
    PERSON,
    US_SSN,
    UK_NHS,
    US_BANK_NUMBER,
    CRYPTO,
    US_PASSPORT
}
