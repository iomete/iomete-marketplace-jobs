#!/usr/bin/env python

"""Tests that rewrite_data_files options reach Iceberg unfiltered.

The job must not implement its own selection algorithm and must not maintain an allow-list.
Iceberg owns which data files qualify; we only pass options through.

Mocked Spark only. These tests must never need a real Spark session.
"""

import os
import tempfile

from unittest.mock import MagicMock

from config import TableMetadata
from data_compaction_job.config import ApplicationConfig, RewriteDataFilesConfig, get_config
from data_compaction_job.sql_compaction import SqlCompaction

TEST_CATALOG = "spark_catalog"
TEST_DATABASE = "test_db"
TEST_TABLE = "test_table"


def build_query(rewrite_data_files: RewriteDataFilesConfig, table_overrides: dict = None) -> str:
    """Run __rewrite_data_files against a mocked Spark and return the single generated query."""
    config = ApplicationConfig(catalog=TEST_CATALOG, rewrite_data_files=rewrite_data_files)

    sql_calls = []
    row = MagicMock()
    row.asDict.return_value = {}
    result = MagicMock()
    result.collect.return_value = [row]

    spark = MagicMock()
    spark.sql.side_effect = lambda query: sql_calls.append(query) or result

    compaction = SqlCompaction(spark, config)
    table_metadata = TableMetadata(
        catalog=TEST_CATALOG,
        database=TEST_DATABASE,
        table=TEST_TABLE,
        table_overrides=table_overrides,
    )

    compaction._SqlCompaction__rewrite_data_files(table_metadata)

    assert len(sql_calls) == 1, f"Expected 1 SQL call, got {len(sql_calls)}"
    return sql_calls[0]


class TestOptionPassthrough:

    def test_arbitrary_option_keys_are_not_filtered(self):
        """No IOMETE allow-list: a key we have never heard of must still reach Iceberg."""
        query = build_query(RewriteDataFilesConfig(options={"some-future-iceberg-option": "42"}))
        assert "'some-future-iceberg-option', '42'" in query

    def test_delete_file_threshold_passes_through(self):
        query = build_query(RewriteDataFilesConfig(options={"delete-file-threshold": 5}))
        assert "'delete-file-threshold', '5'" in query

    def test_delete_ratio_threshold_passes_through(self):
        query = build_query(RewriteDataFilesConfig(options={"delete-ratio-threshold": 0.1}))
        assert "'delete-ratio-threshold', '0.1'" in query

    def test_rewrite_all_passes_through_as_lowercase_true(self):
        query = build_query(RewriteDataFilesConfig(options={"rewrite-all": True}))
        assert "'rewrite-all', 'true'" in query

    def test_remove_dangling_deletes_passes_through_as_lowercase_true(self):
        query = build_query(RewriteDataFilesConfig(options={"remove-dangling-deletes": True}))
        assert "'remove-dangling-deletes', 'true'" in query

    def test_python_false_renders_as_lowercase_false(self):
        query = build_query(RewriteDataFilesConfig(options={"remove-dangling-deletes": False}))
        assert "'remove-dangling-deletes', 'false'" in query

    def test_non_boolean_strings_are_not_coerced(self):
        """Iceberg parses with Boolean.parseBoolean, so 'yes' is false there. We must not repair it."""
        query = build_query(RewriteDataFilesConfig(options={"remove-dangling-deletes": "yes"}))
        assert "'remove-dangling-deletes', 'yes'" in query
        assert "'true'" not in query

    def test_all_delete_aware_options_together(self):
        query = build_query(RewriteDataFilesConfig(options={
            "delete-file-threshold": 5,
            "delete-ratio-threshold": 0.1,
            "remove-dangling-deletes": True,
            "min-input-files": 2,
        }))
        assert "'delete-file-threshold', '5'" in query
        assert "'delete-ratio-threshold', '0.1'" in query
        assert "'remove-dangling-deletes', 'true'" in query
        assert "'min-input-files', '2'" in query


class TestTableOverrides:

    def test_table_override_options_replace_global_options(self):
        """An override replaces the global options map; it does not merge into it."""
        query = build_query(
            RewriteDataFilesConfig(options={"min-input-files": 2}),
            table_overrides={"rewrite_data_files": {"options": {"delete-file-threshold": 5}}},
        )
        assert "'delete-file-threshold', '5'" in query
        assert "min-input-files" not in query

    def test_global_options_used_when_table_has_no_override(self):
        query = build_query(
            RewriteDataFilesConfig(options={"min-input-files": 2}),
            table_overrides={"rewrite_data_files": {"strategy": "sort"}},
        )
        assert "'min-input-files', '2'" in query


class TestHoconOptionKeys:

    def test_quoted_dotted_option_key_survives_config_and_sql_generation(self):
        """pyhocon keeps the literal quotes on a quoted key; the job must emit exactly one key."""
        conf = """
        {
            catalog: "spark_catalog",
            rewrite_data_files: {
                options: {
                    "partial-progress.enabled": true,
                    "partial-progress.max-commits": 5
                }
            }
        }
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as handle:
            handle.write(conf)
            config_path = handle.name

        try:
            config = get_config(config_path)
            assert set(config.rewrite_data_files.options) == {
                "partial-progress.enabled",
                "partial-progress.max-commits",
            }

            query = build_query(config.rewrite_data_files)
            assert "'partial-progress.enabled', 'true'" in query
            assert "'partial-progress.max-commits', '5'" in query
            assert "ConfigTree" not in query
        finally:
            os.unlink(config_path)
