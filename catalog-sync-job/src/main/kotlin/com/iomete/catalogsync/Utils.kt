package com.iomete.catalogsync

import org.apache.spark.sql.Row
import scala.Option


fun <T> Option<T>.getOrNull(): T? = if (this.isDefined) this.get() else null

fun Row.getTimestamp(indexName: String): Long? {
    val fieldVal = this.getTimestamp(this.schema().fieldIndex(indexName)) ?: return null
    return fieldVal.toInstant().toEpochMilli()
}

fun Row.getLong(indexName: String): Long? {
    if (this.get(this.schema().fieldIndex(indexName)) == null) return null
    return this.getLong(this.schema().fieldIndex(indexName))
}

fun Row.getString(indexName: String): String? {
    return this.getString(this.schema().fieldIndex(indexName))
}

fun Row.get(indexName: String): Any? {
    return this.get(this.schema().fieldIndex(indexName))
}

