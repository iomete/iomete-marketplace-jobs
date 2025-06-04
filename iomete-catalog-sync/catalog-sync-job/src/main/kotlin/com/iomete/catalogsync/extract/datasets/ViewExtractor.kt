package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor

class ViewExtractor(
    private val columnTagExtractor: ColumnTagExtractor,
    catalog: String,
    schema: String,
    table: String
) : TableExtractor, SupportColumnTags {
    private val fullName = "`$catalog`.`$schema`.`$table`"
    override fun extractColumnTags(columns: List<String>): Map<String, List<String>> =
        columnTagExtractor.extract(fullName, columns)

    override val getTableType: String
        get() = "VIEW"
}
