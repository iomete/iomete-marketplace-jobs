package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistory
import io.mockk.every
import io.mockk.mockk

/**
 * A checker that holds nothing, for tests of the checks before and after the storage holds. Tests of the holds use
 * the real [StorageHoldChecker].
 */
fun passThroughStorageHoldChecker(): StorageHoldChecker =
    mockk<StorageHoldChecker>().also { checker ->
        every { checker.readHistory(any(), any(), any(), any()) } returns AuditHistory.NotRead
        every { checker.check(any(), any(), any(), any(), any(), any()) } answers { StorageHolds(emptyMap(), arg(4)) }
    }
