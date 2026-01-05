package com.iomete.catalogsync

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.iomete.catalogsync.metadata.CatalogMetadata
import com.iomete.catalogsync.metadata.MetadataScraper
import com.iomete.catalogsync.metadata.SchemaMetadata
import com.iomete.catalogsync.metadata.TableMetadata
import jakarta.inject.Singleton
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient

@Singleton
@RegisterRestClient(configKey = "core-service")
interface CoreClient {
    @GET
    @Path("/internal/v1/spark-settings/catalogs")
    fun catalogs(): List<CatalogDetails>

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class CatalogDetails(
        val name: String,
        val type: List<String>,
        val location: String? = null,
        val storageEndpoint: String? = null,
        val sparkProperties: Map<String, Any?> = emptyMap(),
    )
}

@Singleton
@RegisterRestClient(configKey = "catalog-service")
interface CatalogClient {
    @POST
    @Path("/internal/v2/data-catalog/index/table")
    fun indexTable(data: TableMetadata): Response

    @POST
    @Path("/internal/v2/data-catalog/index/schema")
    fun indexSchema(data: SchemaMetadata): Response

    @POST
    @Path("/internal/v2/data-catalog/index/catalog")
    fun indexCatalog(data: CatalogMetadata): Response
}
