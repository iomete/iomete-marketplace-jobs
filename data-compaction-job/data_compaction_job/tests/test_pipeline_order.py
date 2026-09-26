#!/usr/bin/env python

"""Tests for the order in which maintenance operations are issued for one table.

Mocked Spark only. These tests must never need a real Spark session.
"""

import re

import pytest
from unittest.mock import MagicMock

from config import TableMetadata
from data_compaction_job.config import ApplicationConfig
from data_compaction_job.sql_compaction import SqlCompaction

TEST_CATALOG = "spark_catalog"
TEST_DATABASE = "test_db"
TEST_TABLE = "test_table"

# Expected operation order. rewrite_data_files rewrites data manifests for every partition it
# touches, so a manifest rewrite before it is discarded.
CONTRACT_ORDER = [
    "rewrite_data_files",
    "rewrite_manifests",
    "expire_snapshots",
    "remove_orphan_files",
]

PROCEDURE = re.compile(r"system\.(\w+)\(")


class FakeRow(dict):
    """Supports both row['col'] and row.asDict(), which is all emit_stats needs."""

    def asDict(self):
        return dict(self)


def all_enabled_config() -> ApplicationConfig:
    return ApplicationConfig(catalog=TEST_CATALOG)


def disable(config: ApplicationConfig, operation: str) -> None:
    attribute = {
        "rewrite_data_files": config.rewrite_data_files,
        "rewrite_manifests": config.rewrite_manifests,
        "expire_snapshots": config.expire_snapshot,
        "remove_orphan_files": config.remove_orphan_files,
    }[operation]
    attribute.enabled = False


def executed_operations(config: ApplicationConfig) -> list[str]:
    """Run the per-table pipeline against a mocked Spark and return procedure names in call order."""
    sql_calls = []
    result = MagicMock()
    result.collect.return_value = [FakeRow({"orphan_file_location": "s3://bucket/orphan.parquet"})]

    spark = MagicMock()
    spark.sql.side_effect = lambda query: sql_calls.append(query) or result

    compaction = SqlCompaction(spark, config)
    table_metadata = TableMetadata(catalog=TEST_CATALOG, database=TEST_DATABASE, table=TEST_TABLE)

    compaction._SqlCompaction__run_compaction_operations(table_metadata)

    return [match.group(1) for query in sql_calls for match in [PROCEDURE.search(query)] if match]


class TestPipelineOrder:

    def test_all_enabled_operations_execute_in_contract_order(self):
        assert executed_operations(all_enabled_config()) == CONTRACT_ORDER

    def test_rewrite_data_files_runs_before_rewrite_manifests(self):
        operations = executed_operations(all_enabled_config())
        assert operations.index("rewrite_data_files") < operations.index("rewrite_manifests")

    def test_rewrite_manifests_runs_before_expire_snapshots(self):
        operations = executed_operations(all_enabled_config())
        assert operations.index("rewrite_manifests") < operations.index("expire_snapshots")

    def test_expire_snapshots_runs_before_remove_orphan_files(self):
        operations = executed_operations(all_enabled_config())
        assert operations.index("expire_snapshots") < operations.index("remove_orphan_files")

    def test_remove_orphan_files_is_the_final_operation(self):
        assert executed_operations(all_enabled_config())[-1] == "remove_orphan_files"

    @pytest.mark.parametrize("disabled", CONTRACT_ORDER)
    def test_relative_order_holds_when_one_operation_is_disabled(self, disabled):
        config = all_enabled_config()
        disable(config, disabled)

        operations = executed_operations(config)

        assert disabled not in operations
        assert operations == [operation for operation in CONTRACT_ORDER if operation != disabled]

    def test_only_rewrite_data_files_enabled_issues_one_call(self):
        config = all_enabled_config()
        for operation in ("rewrite_manifests", "expire_snapshots", "remove_orphan_files"):
            disable(config, operation)

        assert executed_operations(config) == ["rewrite_data_files"]
