package com.iomete.cleanup.untrackedtablefolders.audit

// Which reads of other storage finished for one database. A read that was skipped or failed is false, so an audit
// row or report never claims a check that did not run.
data class OwnershipChecks(
    val otherDatabaseFolders: Boolean = false,
    val otherTables: Boolean = false,
    val finalRecheck: Boolean = false,
    // Why the storage of other databases is unknown, for example "catalog=sales_rest: the database list could not be
    // read (ConnectException)". Reading stops at the first one, so a later one shows up only once that is fixed.
    val unreadableStorage: List<String> = emptyList(),
)
