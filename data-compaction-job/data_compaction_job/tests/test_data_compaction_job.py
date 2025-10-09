#!/usr/bin/env python

"""Tests for `data_compaction_job` package."""

from unittest.mock import patch, MagicMock

from data_compaction_job.config import get_config
from data_compaction_job.main import start_job
from data_compaction_job.stats_emitter import init_emitter, _add_orphan_files_metrics, StatsBatcher
from data_compaction_job.tests._spark_session import get_spark_session


@patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
def test_spark_session(mock_catalogs):
    config = get_config("application.conf")

    # create test spark instance
    spark = get_spark_session()

    # Create tables
    spark.sql(f"create database if not exists default")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS default.copy_on_write_table (
        id BIGINT,
        name STRING,
        age INT,
        zipcode STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zipcode)
    TBLPROPERTIES (
        'write.update.mode' = 'copy-on-write',
        'write.merge.mode' = 'copy-on-write',
        'write.delete.mode' = 'copy-on-write'
    )""")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS default.merge_on_read_table (
        id BIGINT,
        name STRING,
        age INT,
        zipcode STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zipcode)
    TBLPROPERTIES (
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read'
    )""")

    # Insert data
    spark.sql("""
    INSERT INTO default.copy_on_write_table (id, name, age, zipcode, timestamp)
VALUES
  (1, 'Alice', 30, '111111', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (2, 'Bob', 25, '111111', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (3, 'Charlie', 35, '111111', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (4, 'David', 28, '111111', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (5, 'Eve', 32, '111111', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (6, 'Frank', 40, '111111', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (7, 'Grace', 22, '111111', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (8, 'Hank', 45, '111111', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (9, 'Ivy', 38, '111111', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (10, 'Jack', 29, '111111', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (11, 'Alice', 30, '222222', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (12, 'Bob', 25, '222222', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (13, 'Charlie', 35, '222222', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (14, 'David', 28, '222222', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (15, 'Eve', 32, '222222', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (16, 'Frank', 40, '222222', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (17, 'Grace', 22, '222222', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (18, 'Hank', 45, '222222', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (19, 'Ivy', 38, '222222', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (20, 'Jack', 29, '222222', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (21, 'Alice', 30, '333333', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (22, 'Bob', 25, '333333', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (23, 'Charlie', 35, '333333', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (24, 'David', 28, '333333', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (25, 'Eve', 32, '333333', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (26, 'Frank', 40, '333333', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (27, 'Grace', 22, '333333', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (28, 'Hank', 45, '333333', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (29, 'Ivy', 38, '333333', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (30, 'Jack', 29, '333333', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (31, 'Alice', 30, '444444', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (32, 'Bob', 25, '444444', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (33, 'Charlie', 35, '444444', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (34, 'David', 28, '444444', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (35, 'Eve', 32, '444444', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (36, 'Frank', 40, '444444', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (37, 'Grace', 22, '444444', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (38, 'Hank', 45, '444444', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (39, 'Ivy', 38, '444444', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (40, 'Jack', 29, '444444', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (41, 'Alice', 30, '555555', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (42, 'Bob', 25, '555555', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (43, 'Charlie', 35, '555555', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (44, 'David', 28, '555555', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (45, 'Eve', 32, '555555', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (46, 'Frank', 40, '555555', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (47, 'Grace', 22, '555555', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (48, 'Hank', 45, '555555', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (49, 'Ivy', 38, '555555', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (50, 'Jack', 29, '555555', CAST('2023-10-19 19:00:00' AS TIMESTAMP))
    """)

    spark.sql("""
    INSERT INTO default.merge_on_read_table (id, name, age, zipcode, timestamp)
VALUES
  (1, 'Alice', 30, '111111', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (2, 'Bob', 25, '111111', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (3, 'Charlie', 35, '111111', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (4, 'David', 28, '111111', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (5, 'Eve', 32, '111111', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (6, 'Frank', 40, '111111', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (7, 'Grace', 22, '111111', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (8, 'Hank', 45, '111111', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (9, 'Ivy', 38, '111111', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (10, 'Jack', 29, '111111', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (11, 'Alice', 30, '222222', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (12, 'Bob', 25, '222222', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (13, 'Charlie', 35, '222222', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (14, 'David', 28, '222222', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (15, 'Eve', 32, '222222', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (16, 'Frank', 40, '222222', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (17, 'Grace', 22, '222222', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (18, 'Hank', 45, '222222', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (19, 'Ivy', 38, '222222', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (20, 'Jack', 29, '222222', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (21, 'Alice', 30, '333333', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (22, 'Bob', 25, '333333', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (23, 'Charlie', 35, '333333', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (24, 'David', 28, '333333', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (25, 'Eve', 32, '333333', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (26, 'Frank', 40, '333333', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (27, 'Grace', 22, '333333', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (28, 'Hank', 45, '333333', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (29, 'Ivy', 38, '333333', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (30, 'Jack', 29, '333333', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (31, 'Alice', 30, '444444', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (32, 'Bob', 25, '444444', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (33, 'Charlie', 35, '444444', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (34, 'David', 28, '444444', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (35, 'Eve', 32, '444444', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (36, 'Frank', 40, '444444', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (37, 'Grace', 22, '444444', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (38, 'Hank', 45, '444444', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (39, 'Ivy', 38, '444444', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (40, 'Jack', 29, '444444', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (41, 'Alice', 30, '555555', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (42, 'Bob', 25, '555555', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (43, 'Charlie', 35, '555555', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (44, 'David', 28, '555555', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (45, 'Eve', 32, '555555', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (46, 'Frank', 40, '555555', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (47, 'Grace', 22, '555555', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (48, 'Hank', 45, '555555', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (49, 'Ivy', 38, '555555', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (50, 'Jack', 29, '555555', CAST('2023-10-19 19:00:00' AS TIMESTAMP))
  """)

    # Post insert validation
    insert_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    insert_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert insert_cow_df.count() == insert_mor_df.count()
    assert insert_cow_df.subtract(insert_mor_df).count() == 0
    assert insert_mor_df.subtract(insert_cow_df).count() == 0

    # Update data
    spark.sql("""
    UPDATE default.copy_on_write_table
    SET age = age + 1
    WHERE (zipcode = '111111' AND id IN (1, 2)) OR
        (zipcode = '222222' AND id IN (11, 12)) OR
        (zipcode = '333333' AND id IN (21, 22)) OR
        (zipcode = '444444' AND id IN (31, 32)) OR
        (zipcode = '555555' AND id IN (41, 42))
    """)

    spark.sql("""
    UPDATE default.merge_on_read_table
    SET age = age + 1
    WHERE (zipcode = '111111' AND id IN (1, 2)) OR
        (zipcode = '222222' AND id IN (11, 12)) OR
        (zipcode = '333333' AND id IN (21, 22)) OR
        (zipcode = '444444' AND id IN (31, 32)) OR
        (zipcode = '555555' AND id IN (41, 42))
    """)

    # Post update validation
    update_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    update_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert update_cow_df.count() == update_mor_df.count()
    assert update_cow_df.subtract(update_mor_df).count() == 0
    assert update_mor_df.subtract(update_cow_df).count() == 0

    # Run compaction job
    start_job(spark, config)

    # Post compaction validation
    compaction_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    compaction_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert compaction_cow_df.count() == compaction_mor_df.count()
    assert compaction_cow_df.subtract(compaction_mor_df).count() == 0
    assert compaction_mor_df.subtract(compaction_cow_df).count() == 0

    assert compaction_cow_df.count() == update_cow_df.count()
    assert compaction_cow_df.subtract(update_cow_df).count() == 0
    assert update_cow_df.subtract(compaction_cow_df).count() == 0

    # Run compaction job again
    start_job(spark, config)

    # Post second compaction validation
    compaction_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    compaction_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert compaction_cow_df.count() == compaction_mor_df.count()
    assert compaction_cow_df.subtract(compaction_mor_df).count() == 0
    assert compaction_mor_df.subtract(compaction_cow_df).count() == 0

    assert compaction_cow_df.count() == update_cow_df.count()
    assert compaction_cow_df.subtract(update_cow_df).count() == 0
    assert update_cow_df.subtract(compaction_cow_df).count() == 0

    # Clean up test tables
    spark.sql("DROP TABLE IF EXISTS default.copy_on_write_table")
    spark.sql("DROP TABLE IF EXISTS default.merge_on_read_table")


@patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
def test_gc_handling_feature(mock_catalogs):
    """Test the G.C. handling feature when G.C. is disabled for a table."""
    config = get_config("application.conf")
    config.gc_handling.enabled = True

    # create test spark instance
    spark = get_spark_session()

    # Create test database and table with G.C. disabled
    spark.sql("CREATE DATABASE IF NOT EXISTS test_gc")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS test_gc.disabled_gc_table (
        id BIGINT,
        name STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'gc.enabled' = 'false'
    )""")

    # Insert some data
    spark.sql("""
    INSERT INTO test_gc.disabled_gc_table VALUES
        (1, 'test1'),
        (2, 'test2'),
        (3, 'test3')
    """)

    # Verify initial state - G.C. should be disabled
    gc_props_before = spark.sql("SHOW TBLPROPERTIES test_gc.disabled_gc_table").collect()
    gc_enabled_before = None
    for row in gc_props_before:
        if row.key.lower() == "gc.enabled":
            gc_enabled_before = row.value.lower()

    assert gc_enabled_before == "false"

    # Capture initial data for comparison
    data_before = spark.sql("SELECT * FROM test_gc.disabled_gc_table").collect()

    # Run compaction job
    start_job(spark, config)

    # Verify data is intact after compaction
    data_after = spark.sql("SELECT * FROM test_gc.disabled_gc_table").collect()
    assert len(data_before) == len(data_after)

    # Verify G.C. was restored to disabled state
    gc_props_after = spark.sql("SHOW TBLPROPERTIES test_gc.disabled_gc_table").collect()
    gc_enabled_after = None
    for row in gc_props_after:
        if row.key.lower() == "gc.enabled":
            gc_enabled_after = row.value.lower()

    assert gc_enabled_after == "false"

    # Clean up test table
    spark.sql("DROP TABLE IF EXISTS test_gc.disabled_gc_table")
    spark.sql("DROP DATABASE IF EXISTS test_gc")


@patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
def test_orphan_files_metrics_tracking(mock_catalogs):
    """Test that orphan files metrics now track both count and exact file paths."""
    config = get_config("application.conf")
    spark = get_spark_session()

    init_emitter(spark)

    # Create test database and table
    spark.sql("CREATE DATABASE IF NOT EXISTS test_orphan")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS test_orphan.orphan_test_table (
        id BIGINT,
        name STRING
    )
    USING iceberg
    """)

    # Insert some data to create files
    spark.sql("""
    INSERT INTO test_orphan.orphan_test_table VALUES
        (1, 'test1'),
        (2, 'test2'),
        (3, 'test3')
    """)

    # Run complete compaction job
    start_job(spark, config)

    # Give some time for the metrics to be processed
    import time
    time.sleep(0.1)

    # Check that metrics were stored correctly
    metrics_df = spark.sql("""
    SELECT metrics FROM spark_catalog.iomete_system_db.table_optimisation_run_metrics 
    WHERE operation = 'REMOVE_ORPHAN_FILES' 
    AND catalog_name = 'spark_catalog' 
    AND database_name = 'test_orphan' 
    AND table_name = 'orphan_test_table'
    ORDER BY start_time DESC
    LIMIT 1
    """)

    metrics_rows = metrics_df.collect()
    assert len(metrics_rows) > 0, "No metrics found for REMOVE_ORPHAN_FILES operation"

    metrics_map = metrics_rows[0]['metrics']

    # Verify that we have both count and file paths metrics
    assert 'removed file count' in metrics_map
    assert 'removed files' in metrics_map

    # The count should be '0' since there are no orphan files in a clean table
    removed_count = int(metrics_map['removed file count'])
    removed_files = metrics_map['removed files']

    # Verify the structure is correct even when no orphan files exist
    assert isinstance(metrics_map['removed file count'], str)
    assert isinstance(metrics_map['removed files'], str)

    # Clean up test table
    spark.sql("DROP TABLE IF EXISTS test_orphan.orphan_test_table")
    spark.sql("DROP DATABASE IF EXISTS test_orphan")


@patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
def test_operation_enabled_flag_execution(mock_catalogs):
    """Test that operations are executed only when enabled in config."""
    import tempfile
    import os

    # Create config with some operations disabled
    config_content = """
    {
        catalog: "spark_catalog"
        expire_snapshot: {
            enabled: false
            retain_last: 1
        }
        rewrite_data_files: {
            enabled: true
            options: {
                "min-input-files": 2
            }
        }
        rewrite_manifests: {
            enabled: false
        }
        remove_orphan_files: {
            enabled: true
            older_than_days: 1
        }
    }
    """

    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        config = get_config(config_file)
        spark = get_spark_session()

        # Create test table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_enabled")
        spark.sql("""
        CREATE TABLE IF NOT EXISTS test_enabled.test_table (
            id BIGINT,
            name STRING
        )
        USING iceberg
        """)

        spark.sql("""
        INSERT INTO test_enabled.test_table VALUES
            (1, 'test1'),
            (2, 'test2')
        """)

        # Use mock.patch to track SQL calls - operations will run SQL queries
        from unittest.mock import Mock, patch as mock_patch
        from data_compaction_job.sql_compaction import SqlCompaction

        compaction = SqlCompaction(spark, config)

        # Track which SQL queries are executed
        sql_calls = []
        original_sql = spark.sql

        def track_sql(query):
            sql_calls.append(query)
            return original_sql(query)

        spark.sql = track_sql

        try:
            # Run compaction operations
            compaction._SqlCompaction__run_compaction_operations("spark_catalog", "test_enabled", "test_table")

            # Check which operations were called by looking at SQL queries
            expire_called = any("expire_snapshots" in call for call in sql_calls)
            rewrite_data_called = any("rewrite_data_files" in call for call in sql_calls)
            rewrite_manifest_called = any("rewrite_manifests" in call for call in sql_calls)
            remove_orphan_called = any("remove_orphan_files" in call for call in sql_calls)

            # Verify only enabled operations were called
            assert not expire_called, "expire_snapshots should not be called (disabled in config)"
            assert rewrite_data_called, "rewrite_data_files should be called (enabled in config)"
            assert not rewrite_manifest_called, "rewrite_manifest should not be called (disabled in config)"
            assert remove_orphan_called, "remove_orphan_files should be called (enabled in config)"
        finally:
            spark.sql = original_sql

        # Clean up
        spark.sql("DROP TABLE IF EXISTS test_enabled.test_table")
        spark.sql("DROP DATABASE IF EXISTS test_enabled")
    finally:
        os.unlink(config_file)


@patch('data_compaction_job.sql_compaction.SqlClient.catalogs', return_value={'spark_catalog'})
def test_operation_enabled_with_table_overrides(mock_catalogs):
    """Test that table-level enabled overrides work correctly."""
    import tempfile
    import os

    # Create config with table-specific overrides
    config_content = """
    {
        catalog: "spark_catalog"
        expire_snapshot: {
            enabled: true
            retain_last: 1
        }
        rewrite_data_files: {
            enabled: true
            options: {
                "min-input-files": 2
            }
        }
        table_overrides: {
            test_override.special_table: {
                expire_snapshot: {
                    enabled: false
                }
                rewrite_data_files: {
                    enabled: false
                }
            }
        }
    }
    """

    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        config = get_config(config_file)
        spark = get_spark_session()

        from data_compaction_job.sql_compaction import SqlCompaction
        from data_compaction_job.constants import CompactionOperation
        from data_compaction_job.decorators import is_operation_enabled

        compaction = SqlCompaction(spark, config)

        # Test that operations are enabled for normal tables
        assert is_operation_enabled(config, "test_override", "normal_table",
                                   CompactionOperation.EXPIRE_SNAPSHOT) is True
        assert is_operation_enabled(config, "test_override", "normal_table",
                                   CompactionOperation.REWRITE_DATA_FILES) is True

        # Test that operations are disabled for the special table with overrides
        assert is_operation_enabled(config, "test_override", "special_table",
                                   CompactionOperation.EXPIRE_SNAPSHOT) is False
        assert is_operation_enabled(config, "test_override", "special_table",
                                   CompactionOperation.REWRITE_DATA_FILES) is False

        # Operations not overridden should remain enabled
        assert is_operation_enabled(config, "test_override", "special_table",
                                   CompactionOperation.REWRITE_MANIFESTS) is True
        assert is_operation_enabled(config, "test_override", "special_table",
                                   CompactionOperation.REMOVE_ORPHAN_FILES) is True

    finally:
        os.unlink(config_file)


def test_timer_decorator():
    """Test that the timer decorator correctly times execution and logs messages."""
    import time
    from unittest.mock import MagicMock
    from data_compaction_job.decorators import timer

    # Create a mock logger to capture log messages
    mock_logger = MagicMock()

    # Patch the logger in the decorators module
    import data_compaction_job.decorators as decorators_module
    original_logger = decorators_module.logger
    decorators_module.logger = mock_logger

    try:
        # Create a test function that takes some time
        @timer("test operation")
        def test_function(x, y):
            time.sleep(0.1)  # Sleep for 100ms
            return x + y

        # Call the function
        start = time.time()
        result = test_function(3, 5)
        elapsed = time.time() - start

        # Verify the function returned the correct result
        assert result == 8, "Timer decorator should preserve function return value"

        # Verify the function took at least 100ms
        assert elapsed >= 0.1, "Function should have taken at least 100ms"

        # Verify debug log was called with start message
        mock_logger.debug.assert_called_once_with("test operation started")

        # Verify info log was called with completion message
        assert mock_logger.info.call_count == 1
        info_call_args = mock_logger.info.call_args[0][0]
        assert "test operation completed in" in info_call_args
        assert "seconds" in info_call_args

        # Extract the duration from the log message
        import re
        match = re.search(r'completed in ([\d.]+) seconds', info_call_args)
        assert match is not None, "Completion message should contain duration"
        logged_duration = float(match.group(1))
        assert logged_duration >= 0.1, "Logged duration should be at least 0.1 seconds"

    finally:
        # Restore original logger
        decorators_module.logger = original_logger


if __name__ == '__main__':
    test_spark_session()
    test_gc_handling_feature()
    test_orphan_files_metrics_tracking()
    test_operation_enabled_flag_execution()
    test_operation_enabled_with_table_overrides()
    test_timer_decorator()
