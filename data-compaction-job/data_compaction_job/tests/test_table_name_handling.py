#!/usr/bin/env python

"""Unit tests for table name handling with and without database prefix."""

from unittest.mock import patch
from data_compaction_job.config import ApplicationConfig, IncludeExcludeConfig
from data_compaction_job.constants import CompactionOperation, ConfigProperty
from data_compaction_job.sql_compaction import SqlCompaction
from data_compaction_job.table_parser import get_table_metadata, get_config_overrides
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
                CompactionOperation.EXPIRE_SNAPSHOT.value: {
                    ConfigProperty.RETAIN_LAST.value: 5
                }
            }
        }

        table_metadata = get_table_metadata(
            catalog="spark_catalog",
            database="db1",
            table="table1",
            table_overrides=config.table_overrides
        )
        result = get_config_overrides(
            table_metadata.table_overrides,
            CompactionOperation.EXPIRE_SNAPSHOT,
            ConfigProperty.RETAIN_LAST
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
                CompactionOperation.EXPIRE_SNAPSHOT.value: {
                    ConfigProperty.RETAIN_LAST.value: 5
                }
            }
        }

        # Should work for any database
        table_metadata1 = get_table_metadata(
            catalog="spark_catalog",
            database="db1",
            table="table1",
            table_overrides=config.table_overrides
        )
        result1 = get_config_overrides(
            table_metadata1.table_overrides,
            CompactionOperation.EXPIRE_SNAPSHOT,
            ConfigProperty.RETAIN_LAST
        )

        table_metadata2 = get_table_metadata(
            catalog="spark_catalog",
            database="db2",
            table="table1",
            table_overrides=config.table_overrides
        )
        result2 = get_config_overrides(
            table_metadata2.table_overrides,
            CompactionOperation.EXPIRE_SNAPSHOT,
            ConfigProperty.RETAIN_LAST
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
                CompactionOperation.EXPIRE_SNAPSHOT.value: {
                    ConfigProperty.RETAIN_LAST.value: 3
                }
            },
            "db1.table1": {
                CompactionOperation.EXPIRE_SNAPSHOT.value: {
                    ConfigProperty.RETAIN_LAST.value: 10
                }
            }
        }

        # db1.table1 should get the specific override (10)
        table_metadata1 = get_table_metadata(
            catalog="spark_catalog",
            database="db1",
            table="table1",
            table_overrides=config.table_overrides
        )
        result1 = get_config_overrides(
            table_metadata1.table_overrides,
            CompactionOperation.EXPIRE_SNAPSHOT,
            ConfigProperty.RETAIN_LAST
        )

        # db2.table1 should get the general override (3)
        table_metadata2 = get_table_metadata(
            catalog="spark_catalog",
            database="db2",
            table="table1",
            table_overrides=config.table_overrides
        )
        result2 = get_config_overrides(
            table_metadata2.table_overrides,
            CompactionOperation.EXPIRE_SNAPSHOT,
            ConfigProperty.RETAIN_LAST
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
        with patch('data_compaction_job.table_parser.logger') as mock_logger:
            table_includes = compaction._SqlCompaction__get_table_includes()

            # Warning should be logged
            assert mock_logger.warning.called
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "without database prefix" in warning_msg
            assert "table1" in warning_msg
