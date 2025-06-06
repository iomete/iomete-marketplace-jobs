package com.iomete.catalogsync

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.inject.Singleton
import jakarta.ws.rs.client.Client
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


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

    private fun <T> sync(endpointPath: String, metadata: T): Boolean {
        val endpoint = "${applicationConfig.catalogEndpoint()}/internal/v2/$endpointPath"
        try {
            val response = client.target(endpoint)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(mapper.writeValueAsString(metadata)))

            if (response.status == Response.Status.NO_CONTENT.statusCode) {
                return true
            }

            logger.error("Unexpected response on syncing: metadata: {}, status: {}", metadata, response.status)
        } catch (ex: RuntimeException) {
            logger.error("Unexpected exception on syncing: metadata: {}", metadata, ex)
            throw ex
        }

        return false
    }

    fun syncTableData(tableMetadata: TableMetadata): Boolean = sync("data-catalog/index/table", tableMetadata)
    fun syncSchemaData(schemaMetadata: SchemaMetadata): Boolean = sync("data-catalog/index/schema", schemaMetadata)
    fun syncCatalogData(catalogMetadata: CatalogMetadata): Boolean = sync("data-catalog/index/catalog", catalogMetadata)
}
