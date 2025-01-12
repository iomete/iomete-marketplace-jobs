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
    @Path("/api/internal/domains/{domain}/spark/settings/catalog-names")
    fun catalogs(@PathParam("domain") domainIdentifier: String): Set<String>
}
