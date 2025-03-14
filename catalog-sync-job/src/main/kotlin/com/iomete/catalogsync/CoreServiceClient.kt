package com.iomete.catalogsync

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient
import jakarta.inject.Singleton
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam

@Singleton
@RegisterRestClient(configKey = "core-service")
interface CoreServiceClient {

    @GET
    @Path("/internal/v1/spark-settings/catalogs")
    fun catalogs(): List<CatalogDetails>

    data class CatalogDetails(
        val name: String,
        val type: List<String>,
        val location: String?,
        val storageEndpoint: String?,
        val domainsAllowed: List<String>,
    )
}
