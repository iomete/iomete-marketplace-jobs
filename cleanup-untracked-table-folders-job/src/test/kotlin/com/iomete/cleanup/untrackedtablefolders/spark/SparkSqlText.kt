package com.iomete.cleanup.untrackedtablefolders.spark

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.analysis.UnresolvedNamespace
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.DescribeNamespace
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces
import org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties
import org.apache.spark.sql.catalyst.plans.logical.ShowTables
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampNTZType
import org.apache.spark.sql.types.TimestampType
import scala.collection.JavaConverters
import scala.runtime.AbstractFunction0

internal const val VARIABLE_SUBSTITUTE = "spark.sql.variable.substitute"
internal const val ESCAPED_STRING_LITERALS = "spark.sql.parser.escapedStringLiterals"
internal const val SESSION_TIME_ZONE = "spark.sql.session.timeZone"
internal const val TIMESTAMP_TYPE = "spark.sql.timestampType"

internal val PINNED_PARSER_SETTINGS = mapOf(VARIABLE_SUBSTITUTE to "false", ESCAPED_STRING_LITERALS to "false")

internal data class InsertText(
    val table: List<String>,
    val columns: List<String>,
    val values: Map<String, Any?>,
)

// Reads the SQL text the job sends with Spark 3.5's own SparkSqlParser, the parser a session uses when no extension
// replaces it, under the given settings. Substitution runs inside the parser, as in a session. Only the statement
// shapes the job builds are accepted; anything else fails the read.
internal class SparkSqlText(
    private val sql: String,
    settings: Map<String, String> = PINNED_PARSER_SETTINGS,
) {
    private val conf = SQLConf().also { conf -> settings.forEach { (key, value) -> conf.setConfString(key, value) } }

    val plan: LogicalPlan by lazy { withConf { SparkSqlParser().parsePlan(sql) } }

    fun insert(): InsertText {
        val insert = plan as? InsertIntoStatement ?: error("not an INSERT: ${plan.javaClass.simpleName}")
        check(!insert.overwrite() && !insert.ifPartitionNotExists() && !insert.byName() && insert.partitionSpec().isEmpty) { "not a plain INSERT INTO" }
        val table = insert.table() as? UnresolvedRelation ?: error("not a table name: ${insert.table().javaClass.simpleName}")
        return InsertText(list(table.multipartIdentifier()), list(insert.userSpecifiedCols()), projected(insert.query()))
    }

    fun select(): Map<String, Any?> = projected(plan)

    // The multipart name of a catalog statement the job sends.
    fun catalogStatementName(): List<String> =
        when (val statement = plan) {
            is ShowTables -> namespace(statement.namespace()).also { check(statement.pattern().isEmpty) { "a pattern" } }
            is ShowNamespaces -> namespace(statement.namespace()).also { check(statement.pattern().isEmpty) { "a pattern" } }
            is DescribeNamespace -> namespace(statement.namespace()).also { check(statement.extended()) { "not EXTENDED" } }
            is DescribeRelation -> tableName(statement.relation()).also { check(statement.isExtended && statement.partitionSpec().isEmpty) { "not EXTENDED" } }
            is ShowTableProperties -> tableName(statement.table()).also { check(statement.propertyKey().isEmpty) { "a property key" } }
            else -> error("not a catalog statement the job sends: ${statement.javaClass.simpleName}")
        }

    private fun namespace(plan: LogicalPlan): List<String> = list((plan as? UnresolvedNamespace ?: error("not a namespace: $plan")).multipartIdentifier())

    private fun tableName(plan: LogicalPlan): List<String> = list((plan as? UnresolvedTableOrView ?: error("not a table name: $plan")).multipartIdentifier())

    // One aliased expression per column, selected from no table.
    private fun projected(plan: LogicalPlan): Map<String, Any?> {
        val project = plan as? Project ?: error("not a SELECT list: ${plan.javaClass.simpleName}")
        check(project.child() is OneRowRelation) { "selects from a table" }
        val values = linkedMapOf<String, Any?>()
        list(project.projectList()).forEach { named ->
            val alias = named as? Alias ?: error("an expression with no alias: $named")
            check(!values.containsKey(alias.name())) { "a column is selected twice" }
            values[alias.name()] = value(alias.child())
        }
        return values
    }

    private fun value(expression: Expression): Any? =
        when (expression) {
            is Literal -> literal(expression)
            // The job casts only a null or an empty collection to give it a type.
            is Cast ->
                value(expression.child()).also { value ->
                    check(value == null || value == emptyList<Any?>() || value == emptyMap<Any?, Any?>()) { "a cast of a value: $expression" }
                }
            is UnresolvedFunction -> {
                check(!expression.isDistinct && expression.filter().isEmpty && !expression.ignoreNulls()) { "not a plain call: $expression" }
                val arguments = list(expression.arguments()).map(::value)
                when (list(expression.nameParts()).map { it.lowercase() }) {
                    listOf("array") -> arguments
                    listOf("map") -> {
                        check(arguments.size % 2 == 0) { "a map with an odd number of arguments" }
                        arguments.chunked(2).associate { (key, value) -> key to value }
                    }
                    else -> error("a call the job does not write: $expression")
                }
            }
            else -> error("an expression the job does not write: ${expression.javaClass.simpleName} $expression")
        }

    private fun literal(literal: Literal): Any? {
        val value = literal.value() ?: return null
        val type: DataType = literal.dataType()
        return when (type) {
            is StringType -> value.toString()
            is IntegerType, is LongType, is BooleanType -> value
            // An instant. A TIMESTAMP_NTZ literal reads back as a local date-time, which never equals an instant.
            is TimestampType -> DateTimeUtils.microsToInstant(value as Long)
            is TimestampNTZType -> DateTimeUtils.microsToLocalDateTime(value as Long)
            is NullType -> null
            else -> error("a literal of type $type")
        }
    }

    private fun <T> list(seq: scala.collection.Seq<T>): List<T> = JavaConverters.seqAsJavaList(seq).toList()

    private fun <T> withConf(block: () -> T): T =
        SQLConf.withExistingConf(
            conf,
            object : AbstractFunction0<T>() {
                override fun apply(): T = block()
            },
        )
}
