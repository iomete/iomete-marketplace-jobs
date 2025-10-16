from datetime import datetime, timezone, timedelta

from config import TableMetadata, ExpireSnapshotConfig
from constants import CompactionOperation, ConfigProperty
from table_parser import get_table_config_override


def get_expire_snapshots_query(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> str:
    catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

    timestamp = _get_timestamp(config, table_metadata)
    retain_last = _get_retain_last_value(config, table_metadata)

    options = (f"table => '`{catalog}`.`{database}`.`{table_name}`',"
               f" retain_last => {retain_last},"
               f" older_than => TIMESTAMP '{timestamp}'")

    return f"CALL {catalog}.system.expire_snapshots({options})"


def _get_timestamp(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> datetime:
    older_than_days = (get_table_config_override(table_metadata.table_overrides,
                                                 table_metadata.database,
                                                 table_metadata.table,
                                                 CompactionOperation.EXPIRE_SNAPSHOT.value,
                                                 ConfigProperty.OLDER_THAN_DAYS.value)
                          or config.older_than_days)

    return (
        datetime.now() - timedelta(minutes=5)
        if older_than_days is None
        else datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
    )


def _get_retain_last_value(config: ExpireSnapshotConfig, table_metadata: TableMetadata) -> int:
    return int(get_table_config_override(table_metadata.table_overrides,
                                         table_metadata.database,
                                         table_metadata.table,
                                         CompactionOperation.EXPIRE_SNAPSHOT.value,
                                         ConfigProperty.RETAIN_LAST.value)
               or config.retain_last)
