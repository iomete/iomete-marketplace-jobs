from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

from iomete_oracle_sync.connection.source import OracleConnection
from iomete_oracle_sync.data_sync.sync_strategy.sync_mode import SyncMode, FullLoad, IncrementalLoad


@dataclass
class SyncSource:
  database: str
  schema: str
  tables: List[str]
  include_all_tables: bool = False
  excluded_tables: List[str] = None


@dataclass
class SyncDestination:
  catalog: str
  schema: str


@dataclass
class SyncConfig:
  source: SyncSource
  destination: SyncDestination
  sync_mode: SyncMode


@dataclass
class ApplicationConfig:
  source_connection: OracleConnection
  sync_configs: List[SyncConfig]


def parse_config(config) -> ApplicationConfig:
  source_connection = OracleConnection(
    host=config['source_connection']['host'],
    port=config['source_connection']['port'],
    user_name=config['source_connection']['username'],
    user_pass=config['source_connection']['password']
  )

  def parse_sync_config(sync_config):
    if sync_config["sync_mode"]["type"] == "full_load":
      sync_mode = FullLoad()
    elif sync_config["sync_mode"]["type"] == "incremental_load":
      start_date_offset_days = sync_config["sync_mode"].get("start_date_offset_days")
      start_date = (datetime.today() - timedelta(
        days=start_date_offset_days)).date().isoformat() if start_date_offset_days else None

      sync_mode = IncrementalLoad(
        sync_config["sync_mode"]["primary_key"],
        sync_config["sync_mode"]["partition_column"],
        start_date or sync_config["sync_mode"].get("start_date"),
        sync_config["sync_mode"].get("end_date")
      )
    else:
      raise Exception("Unknown logger mode {}, allowed logger modes are: {}".format(
        sync_config["sync_mode"]["type"], ["full_load", "incremental_snapshot"]))

    source_tables = sync_config["source"]["tables"]

    return SyncConfig(
      source=SyncSource(
        database=sync_config["source"]["database"],
        schema=sync_config["source"]["schema"],
        tables=sync_config["source"]["tables"],
        include_all_tables=source_tables and source_tables[0] == "*",
        excluded_tables=sync_config.get("source.excluded_tables", [])
      ),
      destination=SyncDestination(sync_config["destination"]["catalog"], sync_config["destination"]["schema"]),
      sync_mode=sync_mode
    )

  return ApplicationConfig(
    source_connection=source_connection,
    sync_configs=[parse_sync_config(sync_config) for sync_config in config["syncs"] or []]
  )
