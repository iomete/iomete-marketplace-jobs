package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.LIVE_FILE_REFERENCED
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.LIVE_SET_INCOMPLETE
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.LIVE_UUID
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.LOCATION_ELSEWHERE
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.METADATA_UNREADABLE
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.UUID_MISSING
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.UuidKeepReason.VIEW_METADATA
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IcebergUuidVetoTest {
    private val folder = "s3a://lake/wh/sales/orders_old"
    private val dropped = FolderMetadataFile("$folder/metadata/00001.metadata.json", true, "aaaa-1111", false, folder)

    @Test
    fun `a dropped table's folder with a complete live set gets no veto`() {
        assertEquals(emptySet<UuidKeepReason>(), veto(live = listOf(live("bbbb-2222"))).keepReasons(folder, listOf(dropped)))
    }

    @Test
    fun `each kind of doubtful metadata file keeps the folder`() {
        val veto = veto(live = listOf(live("BBBB-2222")))
        val cases = mapOf(
            dropped.copy(readable = false, uuid = null, location = null) to METADATA_UNREADABLE,
            dropped.copy(uuid = null) to UUID_MISSING,
            dropped.copy(isView = true) to VIEW_METADATA,
            dropped.copy(uuid = "bbbb-2222") to LIVE_UUID,
            dropped.copy(location = "s3a://lake/wh/sales/orders") to LOCATION_ELSEWHERE,
            dropped.copy(location = null) to LOCATION_ELSEWHERE,
        )

        cases.forEach { (file, reason) -> assertEquals(setOf(reason), veto.keepReasons(folder, listOf(dropped, file)), file.toString()) }
    }

    @Test
    fun `a relocated live table that still names files in the folder keeps it`() {
        val relocated = live("cccc-3333").copy(
            location = "s3a://lake/wh/sales/orders",
            referencedFiles = listOf("$folder/metadata/snap-1.avro"),
        )

        assertEquals(setOf(LIVE_FILE_REFERENCED), veto(live = listOf(relocated)).keepReasons("$folder/", listOf(dropped)))
        assertEquals(setOf(LIVE_FILE_REFERENCED), veto(live = listOf(relocated)).keepReasons(folder, emptyList()))
    }

    @Test
    fun `an incomplete live set keeps every folder`() {
        assertEquals(setOf(LIVE_SET_INCOMPLETE), veto(live = emptyList(), complete = false).keepReasons(folder, listOf(dropped)))
        assertEquals(setOf(LIVE_SET_INCOMPLETE), veto(live = emptyList(), complete = false).keepReasons(folder, emptyList()))
    }

    // The veto only adds keeps: more live evidence, or a less complete read, never removes one.
    @Test
    fun `more live evidence never lifts a keep`() {
        val files = listOf(
            dropped,
            dropped.copy(uuid = null),
            dropped.copy(isView = true),
            dropped.copy(readable = false),
            dropped.copy(uuid = "bbbb-2222"),
            dropped.copy(location = "s3a://elsewhere/t"),
        )
        val lives = listOf(
            live("bbbb-2222"),
            live("aaaa-1111"),
            live("dddd").copy(referencedFiles = listOf("$folder/data/x.parquet")),
            live("eeee").copy(metadataFileLocation = "$folder/metadata/00009.metadata.json"),
        )
        val fileSets = (0 until (1 shl files.size)).map { mask -> files.filterIndexed { i, _ -> mask and (1 shl i) != 0 } }
        val liveSets = (0 until (1 shl lives.size)).map { mask -> lives.filterIndexed { i, _ -> mask and (1 shl i) != 0 } }

        fileSets.forEach { fileSet ->
            liveSets.forEach { liveSet ->
                val base = veto(liveSet).keepReasons(folder, fileSet)
                lives.forEach { extra ->
                    assertTrue(veto(liveSet + extra).keepReasons(folder, fileSet).containsAll(base), "$fileSet $liveSet + $extra")
                }
                assertTrue(veto(liveSet, complete = false).keepReasons(folder, fileSet).containsAll(base))
            }
        }
    }

    private fun veto(live: List<LiveIcebergObject>, complete: Boolean = true) = IcebergUuidVeto(live, complete)

    private fun live(uuid: String) = LiveIcebergObject(
        name = listOf("sales", "t_$uuid"),
        uuid = uuid,
        isView = false,
        location = "s3a://lake/wh/sales/t_$uuid",
        metadataFileLocation = "s3a://lake/wh/sales/t_$uuid/metadata/00002.metadata.json",
        referencedFiles = emptyList(),
        nameMapped = false,
    )
}
