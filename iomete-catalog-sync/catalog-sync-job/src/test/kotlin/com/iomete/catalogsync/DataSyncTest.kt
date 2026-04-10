package com.iomete.catalogsync

import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.*
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.client.Invocation
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.jboss.resteasy.client.jaxrs.ResteasyClient
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class DataSyncTest {

    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockMapper: ObjectMapper
    private lateinit var mockClient: ResteasyClient
    private lateinit var mockWebTarget: ResteasyWebTarget
    private lateinit var mockBuilder: Invocation.Builder
    private lateinit var dataSync: DataSync

    @BeforeEach
    fun setup() {
        mockApplicationConfig = mockk()
        mockMapper = mockk()
        mockClient = mockk()
        mockWebTarget = mockk()
        mockBuilder = mockk()

        every { mockApplicationConfig.catalogEndpoint() } returns "http://localhost:8080"

        mockkConstructor(ResteasyClientBuilderImpl::class)
        every { anyConstructed<ResteasyClientBuilderImpl>().connectionTTL(any(), any()) } returns mockk<ResteasyClientBuilderImpl> {
            every { executorService(any()) } returns mockk<ResteasyClientBuilderImpl> {
                every { build() } returns mockClient
            }
        }

        every { mockClient.target(any<String>()) } returns mockWebTarget
        every { mockWebTarget.request(any<MediaType>()) } returns mockBuilder
        every { mockBuilder.accept(any<MediaType>()) } returns mockBuilder

        dataSync = DataSync(mockApplicationConfig, mockMapper)
    }

    @AfterEach
    fun tearDown() {
        unmockkConstructor(ResteasyClientBuilderImpl::class)
    }

    @Test
    fun `syncTableData should call sync with correct endpoint path`() {
        val metadata = mockk<TableMetadata>()
        val response = mockk<Response>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } returns response
        every { response.status } returns Response.Status.NO_CONTENT.statusCode

        dataSync.syncTableData(metadata)

        verify { mockClient.target("http://localhost:8080/internal/v2/data-catalog/index/table") }
    }

    @Test
    fun `syncSchemaData should call sync with correct endpoint path`() {
        val metadata = mockk<SchemaMetadata>()
        val response = mockk<Response>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } returns response
        every { response.status } returns Response.Status.NO_CONTENT.statusCode

        dataSync.syncSchemaData(metadata)

        verify { mockClient.target("http://localhost:8080/internal/v2/data-catalog/index/schema") }
    }

    @Test
    fun `syncCatalogData should call sync with correct endpoint path`() {
        val metadata = mockk<CatalogMetadata>()
        val response = mockk<Response>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } returns response
        every { response.status } returns Response.Status.NO_CONTENT.statusCode

        dataSync.syncCatalogData(metadata)

        verify { mockClient.target("http://localhost:8080/internal/v2/data-catalog/index/catalog") }
    }

    @Test
    fun `sync should return true for 204 status`() {
        val metadata = mockk<TableMetadata>()
        val response = mockk<Response>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } returns response
        every { response.status } returns Response.Status.NO_CONTENT.statusCode

        val result = dataSync.syncTableData(metadata)

        assertTrue(result)
    }

    @Test
    fun `sync should return false for non-204 status`() {
        val metadata = mockk<TableMetadata>()
        val response = mockk<Response>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } returns response
        every { response.status } returns 200

        val result = dataSync.syncTableData(metadata)

        assertFalse(result)
    }

    @Test
    fun `sync should rethrow RuntimeException`() {
        val metadata = mockk<TableMetadata>()
        every { mockMapper.writeValueAsString(metadata) } returns "{}"
        every { mockBuilder.post(any<Entity<String>>()) } throws RuntimeException("connection refused")

        assertThrows(RuntimeException::class.java) {
            dataSync.syncTableData(metadata)
        }
    }
}
