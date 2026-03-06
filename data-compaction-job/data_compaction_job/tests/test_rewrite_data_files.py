#!/usr/bin/env python

"""Unit tests for rewrite_data_files query generation in SqlCompaction."""

from unittest.mock import MagicMock, patch

from config import TableMetadata
from data_compaction_job.config import ApplicationConfig, RewriteDataFilesConfig
from data_compaction_job.sql_compaction import SqlCompaction

TEST_CATALOG = "spark_catalog"
TEST_DATABASE = "test_db"
TEST_TABLE = "test_table"
TABLE_REF = f"'`{TEST_CATALOG}`.`{TEST_DATABASE}`.`{TEST_TABLE}`'"


def run_rewrite(rewrite_data_files: RewriteDataFilesConfig) -> str:
    """Run __rewrite_data_files and return the generated SQL query string."""
    config = ApplicationConfig(catalog=TEST_CATALOG, rewrite_data_files=rewrite_data_files)
    sql_calls = []
    mock_row = MagicMock()
    mock_row.asDict.return_value = {}
    spark = MagicMock()
    spark.sql.side_effect = lambda q: sql_calls.append(q) or MagicMock(collect=lambda: [mock_row])
    compaction = SqlCompaction(spark, config)
    table_metadata = TableMetadata(catalog=TEST_CATALOG, database=TEST_DATABASE, table=TEST_TABLE)
    with patch("data_compaction_job.stats_emitter._stats_batcher", MagicMock(spark_app_id="test")):
        compaction._SqlCompaction__rewrite_data_files(table_metadata)
    assert len(sql_calls) == 1, f"Expected 1 SQL call, got {len(sql_calls)}"
    return sql_calls[0]


class TestRewriteDataFilesQueryGeneration:

    def test_no_strategy_no_options(self):
        query = run_rewrite(RewriteDataFilesConfig())
        assert query == f"CALL `{TEST_CATALOG}`.system.rewrite_data_files(table => {TABLE_REF})"

    def test_sort_strategy_with_sort_order(self):
        """Reproduces the original bug: strategy and sort_order must be quoted."""
        query = run_rewrite(RewriteDataFilesConfig(strategy="sort", sort_order="user_id ASC"))
        assert "strategy => 'sort'" in query
        assert "sort_order => 'user_id ASC'" in query

    def test_sort_strategy_without_sort_order(self):
        query = run_rewrite(RewriteDataFilesConfig(strategy="sort"))
        assert "strategy => 'sort'" in query
        assert "sort_order" not in query

    def test_binpack_strategy_no_sort_order(self):
        query = run_rewrite(RewriteDataFilesConfig(strategy="binpack"))
        assert "strategy => 'binpack'" in query
        assert "sort_order" not in query

    def test_options_are_included(self):
        opts = {"min-input-files": "1", "target-file-size-bytes": "10485760"}
        query = run_rewrite(RewriteDataFilesConfig(options=opts))
        assert "options => map(" in query
        assert "'min-input-files', '1'" in query
        assert "'target-file-size-bytes', '10485760'" in query

    def test_boolean_options_are_lowercased(self):
        """Python bool True/False from HOCON must become 'true'/'false', not 'True'/'False'."""
        query = run_rewrite(RewriteDataFilesConfig(options={"rewrite-all": True, "delete-file-threshold": False}))
        assert "'rewrite-all', 'true'" in query
        assert "'delete-file-threshold', 'false'" in query
        assert "'True'" not in query
        assert "'False'" not in query

    def test_sort_strategy_with_options(self):
        """Reproduces the exact failing config from the bug report."""
        query = run_rewrite(RewriteDataFilesConfig(
            strategy="sort",
            sort_order="user_id ASC",
            options={
                "min-input-files": "1",
                "target-file-size-bytes": "10485760",
                "rewrite-all": "True",
                "max-file-group-size-bytes": "10737418240",
            }
        ))
        assert f"table => {TABLE_REF}" in query
        assert "strategy => 'sort'" in query
        assert "sort_order => 'user_id ASC'" in query
        assert "options => map(" in query

    def test_where_clause_is_included(self):
        query = run_rewrite(RewriteDataFilesConfig(where="id > 100"))
        assert 'where => "id > 100"' in query

    def test_full_query_ordering(self):
        """table, strategy, sort_order, options, where must appear in that order."""
        query = run_rewrite(RewriteDataFilesConfig(
            strategy="sort",
            sort_order="id DESC",
            options={"min-input-files": "2"},
            where="id > 0",
        ))
        positions = [
            query.index("table =>"),
            query.index("strategy =>"),
            query.index("sort_order =>"),
            query.index("options =>"),
            query.index("where =>"),
        ]
        assert positions == sorted(positions), "Parameters are not in expected order"