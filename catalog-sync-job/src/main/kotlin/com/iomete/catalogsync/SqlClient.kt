package com.iomete.catalogsync

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient
import jakarta.inject.Singleton
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path

@Singleton
@RegisterRestClient(configKey = "sql-service")
interface SqlClient {
    @GET
    @Path("/schema/catalogs")
    fun catalogs(): Set<String>
}
