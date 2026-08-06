package com.iomete.backup.stats

import com.iomete.backup.copy.CopyJobSummary
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.CopyStats

enum class RunStatus { RUNNING, SUCCEEDED, FAILED }

/** Filled in as the run proceeds so a run that throws still reports how far it got. */
class RunProgress {
    var filesListed: Long? = null
    var dirsListed: Long? = null
    var bytesSource: Long? = null
    var sourceListingMs: Long? = null
    var summary: CopyJobSummary? = null
    var copy: CopyStats? = null
    var failures: List<CopyResult> = emptyList()
}
