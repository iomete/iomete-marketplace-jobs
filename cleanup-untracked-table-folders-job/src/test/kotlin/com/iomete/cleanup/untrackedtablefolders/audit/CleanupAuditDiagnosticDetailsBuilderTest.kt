package com.iomete.cleanup.untrackedtablefolders.audit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CleanupAuditDiagnosticDetailsBuilderTest {

    private val builder = CleanupAuditDiagnosticDetailsBuilder()

    @Test
    fun `always emits active table location sample and truncation flag`() {
        val details = builder.build(activeTableLocations = emptyList())

        assertEquals("", details["active_table_locations_sample"])
        assertEquals("false", details["active_table_locations_truncated"])
        assertEquals(setOf("active_table_locations_sample", "active_table_locations_truncated"), details.keys)
    }

    @Test
    fun `adds storage folder section only when storage folder paths are present`() {
        val withStorage = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            storageFolderPaths = listOf("s3a://bucket/db/orphan"),
        )

        assertTrue(withStorage.containsKey("storage_folder_paths_sample"))
        assertTrue(withStorage.containsKey("storage_folder_paths_truncated"))

        val withoutStorage = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
        )

        assertFalse(withoutStorage.containsKey("storage_folder_paths_sample"))
        assertFalse(withoutStorage.containsKey("storage_folder_paths_truncated"))
    }

    @Test
    fun `adds candidate folder section only when candidate paths are present`() {
        val withCandidates = builder.build(
            activeTableLocations = emptyList(),
            candidateFolderPaths = listOf("s3a://bucket/db/orphan"),
        )

        assertTrue(withCandidates.containsKey("candidate_folder_paths_sample"))
        assertTrue(withCandidates.containsKey("candidate_folder_paths_truncated"))

        val withoutCandidates = builder.build(activeTableLocations = emptyList())

        assertFalse(withoutCandidates.containsKey("candidate_folder_paths_sample"))
        assertFalse(withoutCandidates.containsKey("candidate_folder_paths_truncated"))
    }

    @Test
    fun `adds non candidate storage folder section only when paths are present`() {
        val withNonCandidates = builder.build(
            activeTableLocations = emptyList(),
            nonCandidateStorageFolderPaths = listOf("s3a://bucket/db/active_table"),
        )

        assertTrue(withNonCandidates.containsKey("non_candidate_storage_folder_paths_sample"))
        assertTrue(withNonCandidates.containsKey("non_candidate_storage_folder_paths_truncated"))

        val withoutNonCandidates = builder.build(activeTableLocations = emptyList())

        assertFalse(withoutNonCandidates.containsKey("non_candidate_storage_folder_paths_sample"))
        assertFalse(withoutNonCandidates.containsKey("non_candidate_storage_folder_paths_truncated"))
    }

    @Test
    fun `truncates samples to first 100 paths and flags truncation`() {
        val manyPaths = (1..150).map { "s3a://bucket/db/folder_${"%03d".format(it)}" }

        val details = builder.build(activeTableLocations = manyPaths)

        val sample = details["active_table_locations_sample"]!!.split("\n")
        assertEquals(100, sample.size)
        assertEquals("s3a://bucket/db/folder_001", sample.first())
        assertEquals("s3a://bucket/db/folder_100", sample.last())
        assertEquals("true", details["active_table_locations_truncated"])
    }

    @Test
    fun `does not flag truncation when sample size exactly matches limit`() {
        val exactlyHundredPaths = (1..100).map { "s3a://bucket/db/folder_${"%03d".format(it)}" }

        val details = builder.build(activeTableLocations = exactlyHundredPaths)

        assertEquals("false", details["active_table_locations_truncated"])
    }

    @Test
    fun `emits all four sections when every input is populated`() {
        val details = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active"),
            storageFolderPaths = listOf("s3a://bucket/db/active", "s3a://bucket/db/orphan"),
            candidateFolderPaths = listOf("s3a://bucket/db/orphan"),
            nonCandidateStorageFolderPaths = listOf("s3a://bucket/db/active"),
        )

        assertEquals(
            setOf(
                "active_table_locations_sample",
                "active_table_locations_truncated",
                "storage_folder_paths_sample",
                "storage_folder_paths_truncated",
                "candidate_folder_paths_sample",
                "candidate_folder_paths_truncated",
                "non_candidate_storage_folder_paths_sample",
                "non_candidate_storage_folder_paths_truncated",
            ),
            details.keys,
        )
    }
}
