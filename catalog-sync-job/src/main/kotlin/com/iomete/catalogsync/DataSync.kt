package com.iomete.catalogsync

import com.fasterxml.jackson.databind.ObjectMapper
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import jakarta.inject.Singleton
import jakarta.ws.rs.client.Client
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response


@Singleton
class DataSync(
    private val applicationConfig: ApplicationConfig,
    private val mapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val executorService = Executors.newCachedThreadPool()

    private val client: Client = ResteasyClientBuilderImpl()
        .connectionTTL(2, TimeUnit.MINUTES)
        .executorService(executorService)
        .build()

    fun syncData(tableMetadata: TableMetadata): Boolean {
        val endpoint = "${applicationConfig.catalogEndpoint()}/internal/v1/data-catalog/domain/${System.getenv("DOMAIN")}/index"
        try {
            val response = client.target(endpoint)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(mapper.writeValueAsString(tableMetadata)))

            if (response.status == Response.Status.NO_CONTENT.statusCode) {
                return true
            }

            logger.error("Unexpected response on syncing: tableMetadata: {}, status: {}", tableMetadata, response.status)
        } catch (ex: RuntimeException) {
            logger.error("Unexpected exception on syncing: tableMetadata: {}", tableMetadata, ex)
            throw ex
        }

        return false
    }
}
