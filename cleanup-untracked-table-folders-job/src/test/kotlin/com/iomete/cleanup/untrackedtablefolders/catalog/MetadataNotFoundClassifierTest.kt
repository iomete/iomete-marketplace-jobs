package com.iomete.cleanup.untrackedtablefolders.catalog

import java.io.FileNotFoundException
import java.net.ConnectException
import java.net.SocketTimeoutException
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MetadataNotFoundClassifierTest {

    private class NoSuchTableException(message: String) : RuntimeException(message)

    private class ServiceFailureException(message: String) : RuntimeException(message)

    private class S3Exception(message: String) : RuntimeException(message)

    @Test
    fun `rest catalog missing metadata signature is unresolved`() {
        val error =
            ServiceFailureException(
                "Server error: NotFoundException: Location does not exist: " +
                    "s3://bucket/db/orders/metadata/00042-abc.metadata.json",
            )

        assertTrue(isMetadataNotFoundError(error))
    }

    @Test
    fun `direct iceberg missing metadata signature is unresolved`() {
        val error = RuntimeException("Location does not exist: s3://bucket/db/orders/metadata/00042-abc.metadata.json")

        assertTrue(isMetadataNotFoundError(error))
    }

    @Test
    fun `missing metadata nested in a cause chain is unresolved`() {
        val error =
            RuntimeException(
                "Failed to load table",
                RuntimeException("Location does not exist: s3://bucket/db/orders/metadata/00042-abc.metadata.json"),
            )

        assertTrue(isMetadataNotFoundError(error))
    }

    @Test
    fun `FileNotFoundException is unresolved`() {
        val error = FileNotFoundException("No such file or directory: s3a://bucket/db/orders/metadata/00042-abc.metadata.json")

        assertTrue(isMetadataNotFoundError(error))
    }

    @Test
    fun `NoSuchTableException is not unresolved`() {
        assertFalse(isMetadataNotFoundError(NoSuchTableException("Table does not exist: analytics.orders")))
    }

    @Test
    fun `access denied is not unresolved`() {
        assertFalse(isMetadataNotFoundError(S3Exception("Access Denied (Service: S3, Status Code: 403, Request ID: X)")))
    }

    @Test
    fun `read timeout is not unresolved`() {
        assertFalse(isMetadataNotFoundError(SocketTimeoutException("Read timed out")))
    }

    @Test
    fun `connection failure is not unresolved`() {
        assertFalse(isMetadataNotFoundError(ConnectException("Connection refused")))
    }

    @Test
    fun `generic server error is not unresolved`() {
        assertFalse(
            isMetadataNotFoundError(
                ServiceFailureException("Server error: IllegalStateException: connection pool exhausted"),
            ),
        )
    }

    @Test
    fun `a generic not found phrase is not unresolved`() {
        assertFalse(isMetadataNotFoundError(RuntimeException("resource not found")))
        assertFalse(isMetadataNotFoundError(RuntimeException("The specified key does not exist")))
        assertFalse(isMetadataNotFoundError(RuntimeException("location does not exist: s3://bucket/db/orders")))
    }

    @Test
    fun `raw NoSuchKey is not matched directly`() {
        assertFalse(isMetadataNotFoundError(RuntimeException("NoSuchKey (Service: S3, Status Code: 404)")))
    }

    @Test
    fun `a null message does not break the cause walk`() {
        assertFalse(isMetadataNotFoundError(RuntimeException()))
    }
}
