from datetime import datetime, timezone, timedelta

from common.config import TableMetadata, ExpireSnapshotConfig
from constants import CompactionOperation, ConfigProperty, ExpireSnapshotDefaults
from table_parser import get_config_overrides


def get_expire_snapshots_query(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> str:
    catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

    timestamp = _get_timestamp(config, table_metadata)
    retain_last = _get_retain_last_value(config, table_metadata)

    options = (f"table => '`{catalog}`.`{database}`.`{table_name}`',"
               f" retain_last => {retain_last},"
               f" older_than => TIMESTAMP '{timestamp}'")

    return f"CALL {catalog}.system.expire_snapshots({options})"


def _get_timestamp(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> datetime:
    override_value = get_config_overrides(table_metadata.table_overrides,
                                            CompactionOperation.EXPIRE_SNAPSHOT,
                                            ConfigProperty.OLDER_THAN_DAYS)

    older_than_days = override_value if override_value is not None else config.older_than_days

    return (
        datetime.now() - timedelta(minutes=ExpireSnapshotDefaults.OLDER_THAN_MINUTES)
        if older_than_days is None
        else datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
    )


def _get_retain_last_value(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> int:
    return int(get_config_overrides(table_metadata.table_overrides,
                                         CompactionOperation.EXPIRE_SNAPSHOT,
                                         ConfigProperty.RETAIN_LAST)
               or config.retain_last)
