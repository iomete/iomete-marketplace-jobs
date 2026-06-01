package com.iomete.cleanup.untrackedtablefolders.audit

/**
 * Maximum number of paths included in the audit-table `candidate_folders` array
 * column and in any `*_sample` entries inside the `diagnostic_details` map.
 *
 * Used by [CleanupAuditRecorder] to pre-truncate array columns and by
 * [CleanupAuditDiagnosticDetailsBuilder] to truncate and flag textual samples.
 * Both must use the same value so that the audit row is internally consistent.
 */
internal const val MAX_AUDIT_PATH_SAMPLE_SIZE = 100
