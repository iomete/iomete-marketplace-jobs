package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.`CatalystSqlParser$`
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.collection.JavaConverters

/**
 * Finds folders a saved view reads by path, such as parquet.`s3a://bucket/folder`. No table owns those folders,
 * and the authorization plugin does not check path reads.
 */
class ViewPathParser(
    private val parse: (String) -> LogicalPlan = { `CatalystSqlParser$`.`MODULE$`.parsePlan(it) },
) {
    /** Returns null when the view reads no path. */
    fun pathUse(name: List<String>, sql: String?): ExternalPathUse? {
        if (sql.isNullOrBlank()) return ExternalPathUse(name, emptyList(), "view_text_missing")
        val plan = try {
            parse(sql)
        } catch (_: Exception) {
            return ExternalPathUse(name, emptyList(), "view_text_unparseable")
        }
        val paths = linkedSetOf<String>()
        collectPlan(plan, paths)
        return if (paths.isEmpty()) null else ExternalPathUse(name, paths.toList())
    }

    // innerChildren holds CTE definitions and the plans of subqueries inside expressions.
    private fun collectPlan(node: QueryPlan<*>, paths: MutableSet<String>) {
        if (node is UnresolvedRelation) {
            val parts = node.multipartIdentifier().asList()
            // Spark resolves <format>.`<path>` as a path read. Any two-part name whose second part looks like a
            // path is counted, which over-holds rather than misses.
            if (parts.size == 2 && (parts[1].contains('/') || parts[1].contains(':'))) paths += parts[1]
        }
        node.children().asList().forEach { collectPlan(it as QueryPlan<*>, paths) }
        node.innerChildren().asList().forEach { collectPlan(it, paths) }
    }
}

internal fun <T> scala.collection.Seq<T>.asList(): List<T> = JavaConverters.seqAsJavaList(this)
