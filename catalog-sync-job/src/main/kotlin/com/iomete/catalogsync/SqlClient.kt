package com.iomete.catalogsync

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient
import jakarta.inject.Singleton
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam

@Singleton
@RegisterRestClient(configKey = "core-service")
interface SqlClient {
    @GET
    @Path("/domain/{domainIdentifier}/catalog-names")
    fun catalogs(@PathParam("domainIdentifier") domainIdentifier: String): Set<String>
}
