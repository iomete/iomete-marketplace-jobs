package com.iomete.cleanup.untrackedtablefolders.storage

import io.quarkus.test.junit.QuarkusTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@QuarkusTest
class StoragePathUtilsTest {
    @Test
    fun `normalizes trailing slash and scheme casing`() {
        assertEquals(
            "s3a://bucket/data/db/table",
            StoragePathUtils.normalizeLocation("S3A://Bucket/data/db/table/"),
        )
    }

    @Test
    fun `normalizes duplicate path separators`() {
        assertEquals(
            "s3a://bucket/data/db/table",
            StoragePathUtils.normalizeLocation("s3a://bucket/data//db///table/"),
        )
    }

    @Test
    fun `detects same or child location`() {
        assertTrue(
            StoragePathUtils.isSameOrChildLocation(
                candidateLocation = "s3a://bucket/data/db/table",
                rootLocation = "s3a://bucket/data/db",
            )
        )

        assertTrue(
            StoragePathUtils.isSameOrChildLocation(
                candidateLocation = "s3a://bucket/data/db",
                rootLocation = "s3a://bucket/data/db",
            )
        )
    }

    @Test
    fun `rejects sibling location`() {
        assertFalse(
            StoragePathUtils.isSameOrChildLocation(
                candidateLocation = "s3a://bucket/data/db_b/table",
                rootLocation = "s3a://bucket/data/db_a",
            )
        )
    }

    @Test
    fun `allows db suffix fallback root`() {
        assertEquals(
            setOf(
                "s3a://bucket/data/db_a.db",
                "s3a://bucket/data/db_a",
            ),
            StoragePathUtils.allowedDatabaseRoots("s3a://bucket/data/db_a.db"),
        )
    }

    @Test
    fun `detects candidate inside db suffix fallback root`() {
        assertTrue(
            StoragePathUtils.isInsideAnyRoot(
                candidateLocation = "s3a://bucket/data/db_a/table",
                rootLocations = StoragePathUtils.allowedDatabaseRoots("s3a://bucket/data/db_a.db"),
            )
        )
    }

    @Test
    fun `rejects scan root escaping database boundary`() {
        assertFalse(
            StoragePathUtils.isInsideAnyRoot(
                candidateLocation = "s3a://bucket/data",
                rootLocations = StoragePathUtils.allowedDatabaseRoots("s3a://bucket/data/db_a"),
            )
        )
    }
}
