package com.iomete.cleanup.untrackedtablefolders.audit

// Why folders that passed every ownership check were still not deleted. The code is the audit status_reason.
enum class DeletionBlockReason(val code: String) {
    AUDIT_INTEGRITY_LOST("audit_integrity_lost"),
    CLEANUP_ROOT_HAS_NO_VISIBLE_TABLE("cleanup_root_has_no_visible_table"),
}
