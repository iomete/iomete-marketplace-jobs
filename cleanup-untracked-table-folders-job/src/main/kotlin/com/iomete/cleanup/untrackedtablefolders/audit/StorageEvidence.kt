package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.candidate.StorageHolds

/**
 * What a row records from the storage checks: the fingerprint of each folder inspected, which later runs compare for
 * confirmation, and the folders the checks held. Null [fingerprints] means the run inspected no folder.
 */
data class StorageEvidence(
    val fingerprints: Map<String, String>?,
    val holds: StorageHolds,
) {
    companion object {
        val NONE = StorageEvidence(null, StorageHolds.NONE)
    }
}
