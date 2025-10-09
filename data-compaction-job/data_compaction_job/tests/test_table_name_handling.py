#!/usr/bin/env python

"""Unit tests for table name handling with and without database prefix."""

from unittest.mock import patch
from data_compaction_job.config import ApplicationConfig, IncludeExcludeConfig
from data_compaction_job.sql_compaction import SqlCompaction
from data_compaction_job.tests._spark_session import get_spark_session


class TestTableNameHandling:
    """Test handling of table names with and without database prefix."""

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_include_with_database_prefix(self, mock_catalogs):
        """Test table_include with database.table format."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = ["db1", "db2"]
        config.include_exclude.table_include = ["db1.table1", "db2.table2"]

        compaction = SqlCompaction(spark, config)
        table_includes = compaction._SqlCompaction__get_table_includes()

        assert "db1" in table_includes
        assert "table1" in table_includes["db1"]
        assert "table2" not in table_includes["db1"]
        assert "db2" in table_includes
        assert "table1" not in table_includes["db2"]
        assert "table2" in table_includes["db2"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_include_without_database_prefix(self, mock_catalogs):
        """Test table_include with just table name (no database prefix)."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = ["db1", "db2"]
        config.include_exclude.table_include = ["table1"]

        compaction = SqlCompaction(spark, config)
        # Simulate setting _databases (as would happen in run_compaction)
        compaction._databases = ["db1", "db2"]
        table_includes = compaction._SqlCompaction__get_table_includes()

        # Table should be applied to all databases in config
        assert "db1" in table_includes
        assert "table1" in table_includes["db1"]
        assert "db2" in table_includes
        assert "table1" in table_includes["db2"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_include_mixed_formats(self, mock_catalogs):
        """Test table_include with mixed formats (with and without prefix)."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = ["db1", "db2"]
        config.include_exclude.table_include = ["db1.specific_table", "common_table"]

        compaction = SqlCompaction(spark, config)
        # Simulate setting _databases (as would happen in run_compaction)
        compaction._databases = ["db1", "db2"]
        table_includes = compaction._SqlCompaction__get_table_includes()

        # Specific table should only be in db1
        assert "db1" in table_includes
        assert "specific_table" in table_includes["db1"]
        assert "common_table" in table_includes["db1"]

        # Common table should be in all databases
        assert "db2" in table_includes
        assert "common_table" in table_includes["db2"]
        assert "specific_table" not in table_includes["db2"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_exclude_with_database_prefix(self, mock_catalogs):
        """Test table_exclude with database.table format."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = ["db1", "db2"]
        config.include_exclude.table_exclude = ["db1.table1", "db2.table2"]

        compaction = SqlCompaction(spark, config)
        table_excludes = compaction._SqlCompaction__get_table_excludes()

        assert "db1" in table_excludes
        assert "table1" in table_excludes["db1"]
        assert "table2" not in table_excludes["db1"]
        assert "db2" in table_excludes
        assert "table1" not in table_excludes["db2"]
        assert "table2" in table_excludes["db2"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_exclude_without_database_prefix(self, mock_catalogs):
        """Test table_exclude with just table name (no database prefix)."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = ["db1", "db2"]
        config.include_exclude.table_exclude = ["table1"]

        compaction = SqlCompaction(spark, config)
        # Simulate setting _databases (as would happen in run_compaction)
        compaction._databases = ["db1", "db2"]
        table_excludes = compaction._SqlCompaction__get_table_excludes()

        # Table should be excluded from all databases in config
        assert "db1" in table_excludes
        assert "table1" in table_excludes["db1"]
        assert "db2" in table_excludes
        assert "table1" in table_excludes["db2"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_override_with_database_prefix(self, mock_catalogs):
        """Test table_overrides with database.table format."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.table_overrides = {
            "db1.table1": {
                "expire_snapshot": {
                    "retain_last": 5
                }
            }
        }

        compaction = SqlCompaction(spark, config)
        result = compaction._SqlCompaction__get_final_config_for_table(
            "db1", "table1", "expire_snapshot", "retain_last"
        )

        assert result == 5

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_override_without_database_prefix(self, mock_catalogs):
        """Test table_overrides with just table name (no database prefix)."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.table_overrides = {
            "table1": {
                "expire_snapshot": {
                    "retain_last": 5
                }
            }
        }

        compaction = SqlCompaction(spark, config)

        # Should work for any database
        result1 = compaction._SqlCompaction__get_final_config_for_table(
            "db1", "table1", "expire_snapshot", "retain_last"
        )
        result2 = compaction._SqlCompaction__get_final_config_for_table(
            "db2", "table1", "expire_snapshot", "retain_last"
        )

        assert result1 == 5
        assert result2 == 5

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_override_priority(self, mock_catalogs):
        """Test that database.table format takes priority over table name alone."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.table_overrides = {
            "table1": {
                "expire_snapshot": {
                    "retain_last": 3
                }
            },
            "db1.table1": {
                "expire_snapshot": {
                    "retain_last": 10
                }
            }
        }

        compaction = SqlCompaction(spark, config)

        # db1.table1 should get the specific override (10)
        result1 = compaction._SqlCompaction__get_final_config_for_table(
            "db1", "table1", "expire_snapshot", "retain_last"
        )

        # db2.table1 should get the general override (3)
        result2 = compaction._SqlCompaction__get_final_config_for_table(
            "db2", "table1", "expire_snapshot", "retain_last"
        )

        assert result1 == 10
        assert result2 == 3

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_include_without_databases_config(self, mock_catalogs):
        """Test table_include with no database prefix and no databases in config uses _databases."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = None  # No databases specified
        config.include_exclude.table_include = ["table1"]

        compaction = SqlCompaction(spark, config)
        # Simulate setting _databases (as would happen in run_compaction)
        compaction._databases = ["db1", "db2", "db3"]

        table_includes = compaction._SqlCompaction__get_table_includes()

        # Table should be applied to all databases from _databases
        assert "db1" in table_includes
        assert "table1" in table_includes["db1"]
        assert "db2" in table_includes
        assert "table1" in table_includes["db2"]
        assert "db3" in table_includes
        assert "table1" in table_includes["db3"]

    @patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
    def test_table_include_without_databases_or_stored_databases(self, mock_catalogs):
        """Test table_include with no database prefix, no databases in config, and no _databases logs warning."""
        spark = get_spark_session()
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.include_exclude = IncludeExcludeConfig()
        config.include_exclude.databases = None  # No databases specified
        config.include_exclude.table_include = ["table1"]

        compaction = SqlCompaction(spark, config)
        # _databases is None (not set yet)

        # Should log a warning and not add the table
        with patch('data_compaction_job.sql_compaction.logger') as mock_logger:
            table_includes = compaction._SqlCompaction__get_table_includes()

            # Warning should be logged
            assert mock_logger.warning.called
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "without database prefix" in warning_msg
            assert "table1" in warning_msg
