#!/usr/bin/env python

"""Unit tests for expire_snapshots module."""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from common.config import TableMetadata, ExpireSnapshotConfig
from constants import CompactionOperation, ConfigProperty, ExpireSnapshotDefaults
from operations.expire_snapshots import get_expire_snapshots_query, _get_timestamp, _get_retain_last_value


# Common test constants
TEST_CATALOG = "spark_catalog"
TEST_DATABASE = "test_db"
TEST_TABLE = "test_table"
MOCK_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def assert_expire_snapshots_query(
    actual_query: str,
    catalog: str,
    database: str,
    table: str,
    retain_last: int,
    timestamp: datetime
):
    """Helper function to assert expire_snapshots query format."""
    expected_query = (
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '`{catalog}`.`{database}`.`{table}`', "
        f"retain_last => {retain_last}, "
        f"older_than => TIMESTAMP '{timestamp}')"
    )
    assert actual_query == expected_query


class TestGetExpireSnapshotsQuery:
    """Unit tests for get_expire_snapshots_query function."""

    @patch('operations.expire_snapshots.datetime')
    def test_basic_query_generation(self, mock_datetime):
        """Test basic query generation with default config."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = MOCK_NOW - timedelta(days=7)
        assert_expire_snapshots_query(
            query, TEST_CATALOG, TEST_DATABASE, TEST_TABLE, 5, expected_timestamp
        )

    @patch('operations.expire_snapshots.datetime')
    def test_query_with_table_overrides_for_retain_last(self, mock_datetime):
        """Test query generation with table-level retain_last override."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.RETAIN_LAST: 10
                }
            }
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = MOCK_NOW - timedelta(days=7)
        assert_expire_snapshots_query(
            query, TEST_CATALOG, TEST_DATABASE, TEST_TABLE, 10, expected_timestamp
        )

    @patch('operations.expire_snapshots.datetime')
    def test_query_with_table_overrides_for_older_than_days(self, mock_datetime):
        """Test query generation with table-level older_than_days override."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.OLDER_THAN_DAYS: 14
                }
            }
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = MOCK_NOW - timedelta(days=14)
        assert_expire_snapshots_query(
            query, TEST_CATALOG, TEST_DATABASE, TEST_TABLE, 5, expected_timestamp
        )

    @patch('operations.expire_snapshots.datetime')
    def test_query_with_both_table_overrides(self, mock_datetime):
        """Test query generation with both retain_last and older_than_days overridden."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.RETAIN_LAST: 10,
                    ConfigProperty.OLDER_THAN_DAYS: 14
                }
            }
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = MOCK_NOW - timedelta(days=14)
        assert_expire_snapshots_query(
            query, TEST_CATALOG, TEST_DATABASE, TEST_TABLE, 10, expected_timestamp
        )

    @patch('operations.expire_snapshots.datetime')
    def test_query_with_different_catalog(self, mock_datetime):
        """Test query generation with non-default catalog."""
        # Mock current time
        mock_now = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=3,
            retain_last=2
        )

        table_metadata = TableMetadata(
            catalog="prod_catalog",
            database="analytics_db",
            table="user_events",
            table_overrides=None
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = mock_now - timedelta(days=3)
        assert_expire_snapshots_query(
            query, "prod_catalog", "analytics_db", "user_events", 2, expected_timestamp
        )


class TestGetTimestamp:
    """Unit tests for _get_timestamp function."""

    @patch('operations.expire_snapshots.datetime')
    def test_timestamp_with_older_than_days_specified(self, mock_datetime):
        """Test timestamp calculation when older_than_days is specified in config."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        timestamp = _get_timestamp(config, table_metadata)

        expected = MOCK_NOW - timedelta(days=7)
        assert timestamp == expected

    @patch('operations.expire_snapshots.datetime')
    def test_timestamp_with_none_older_than_days(self, mock_datetime):
        """Test timestamp defaults to 5 minutes when older_than_days is None."""
        # Mock current time (without timezone for naive datetime)
        mock_now_naive = datetime(2025, 1, 15, 12, 0, 0)
        mock_datetime.now.side_effect = lambda tz=None: mock_now_naive if tz is None else datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=None,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog="spark_catalog",
            database="test_db",
            table="test_table",
            table_overrides=None
        )

        timestamp = _get_timestamp(config, table_metadata)

        expected = mock_now_naive - timedelta(minutes=5)
        assert timestamp == expected

    @patch('operations.expire_snapshots.datetime')
    def test_timestamp_with_table_override(self, mock_datetime):
        """Test timestamp calculation with table-level override."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.OLDER_THAN_DAYS: 30
                }
            }
        )

        timestamp = _get_timestamp(config, table_metadata)

        expected = MOCK_NOW - timedelta(days=30)
        assert timestamp == expected

    @patch('operations.expire_snapshots.datetime')
    def test_timestamp_with_zero_days(self, mock_datetime):
        """Test timestamp calculation when older_than_days is 0."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=0,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        timestamp = _get_timestamp(config, table_metadata)

        expected = MOCK_NOW - timedelta(days=0)
        assert timestamp == expected


class TestGetRetainLastValue:
    """Unit tests for _get_retain_last_value function."""

    def test_retain_last_from_config(self):
        """Test getting retain_last value from config when no override."""
        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        retain_last = _get_retain_last_value(config, table_metadata)

        assert retain_last == 5

    def test_retain_last_from_table_override(self):
        """Test getting retain_last value from table override."""
        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.RETAIN_LAST: 10
                }
            }
        )

        retain_last = _get_retain_last_value(config, table_metadata)

        assert retain_last == 10

    def test_retain_last_with_default_value(self):
        """Test retain_last defaults to ExpireSnapshotDefaults.RETAIN_LAST."""
        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7
            # retain_last not specified, should use default
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        retain_last = _get_retain_last_value(config, table_metadata)

        assert retain_last == ExpireSnapshotDefaults.RETAIN_LAST

    def test_retain_last_converts_to_int(self):
        """Test that retain_last value is converted to int."""
        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.RETAIN_LAST: "15"  # String value
                }
            }
        )

        retain_last = _get_retain_last_value(config, table_metadata)

        assert retain_last == 15
        assert isinstance(retain_last, int)

    def test_retain_last_with_zero_value(self):
        """Test retain_last with zero value."""
        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=0
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table=TEST_TABLE,
            table_overrides=None
        )

        retain_last = _get_retain_last_value(config, table_metadata)

        assert retain_last == 0


class TestExpireSnapshotsIntegration:
    """Integration tests for expire_snapshots module."""

    @patch('operations.expire_snapshots.datetime')
    def test_end_to_end_with_minimal_config(self, mock_datetime):
        """Test end-to-end query generation with minimal configuration."""
        mock_datetime.now.return_value = MOCK_NOW

        # Minimal config
        config = ExpireSnapshotConfig()

        table_metadata = TableMetadata(
            catalog="catalog1",
            database="db1",
            table="table1",
            table_overrides=None
        )

        query = get_expire_snapshots_query(config, table_metadata)

        # Should use default retain_last = 1 and older_than_days = None (5 minutes)
        assert "catalog1.system.expire_snapshots" in query
        assert "catalog1`.`db1`.`table1" in query
        assert "retain_last => 1" in query
        assert "older_than => TIMESTAMP" in query

    @patch('operations.expire_snapshots.datetime')
    def test_end_to_end_with_complete_override(self, mock_datetime):
        """Test end-to-end query generation with complete table override."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database="production",
            table="critical_table",
            table_overrides={
                CompactionOperation.EXPIRE_SNAPSHOT: {
                    ConfigProperty.RETAIN_LAST: 20,
                    ConfigProperty.OLDER_THAN_DAYS: 60
                }
            }
        )

        query = get_expire_snapshots_query(config, table_metadata)

        expected_timestamp = MOCK_NOW - timedelta(days=60)
        assert_expire_snapshots_query(
            query, TEST_CATALOG, "production", "critical_table", 20, expected_timestamp
        )

    @patch('operations.expire_snapshots.datetime')
    def test_table_name_with_special_characters(self, mock_datetime):
        """Test query generation with table names containing special characters."""
        mock_datetime.now.return_value = MOCK_NOW

        config = ExpireSnapshotConfig(
            enabled=True,
            older_than_days=7,
            retain_last=5
        )

        table_metadata = TableMetadata(
            catalog=TEST_CATALOG,
            database=TEST_DATABASE,
            table="table-with-dashes",
            table_overrides=None
        )

        query = get_expire_snapshots_query(config, table_metadata)

        # Verify backticks properly escape the table name
        assert f"`{TEST_CATALOG}`.`{TEST_DATABASE}`.`table-with-dashes`" in query
