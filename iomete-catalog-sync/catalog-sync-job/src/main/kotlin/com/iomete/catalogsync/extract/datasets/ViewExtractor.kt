package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.TableExtractor

class ViewExtractor(
    catalog: String,
    schema: String,
    table: String,
) : TableExtractor,
    SupportColumnTags {
    override val getTableType: String
        get() = "VIEW"
}
