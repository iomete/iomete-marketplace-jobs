import logging

from iomete_oracle_sync.connection.destination import IcebergCatalog
from iomete_oracle_sync.data_sync.sync_strategy.sync_mode import SyncMode, FullLoad, IncrementalLoad

logger = logging.getLogger(__name__)


class DataLoadStrategyFactory:
  @staticmethod
  def instance_for(sync_mode: SyncMode, destination_catalog: IcebergCatalog):
    if isinstance(sync_mode, FullLoad):
      return FullLoadStrategy(destination_catalog)

    if isinstance(sync_mode, IncrementalLoad):
      return IncrementalLoadStrategy(destination_catalog, sync_mode)

    raise Exception(f"Unsupported load type: {sync_mode}")


class DataLoadStrategy:
  def load_data(self, proxy_table, destination_table):
    pass


class IncrementalLoadStrategy(DataLoadStrategy):

  def __init__(self, destination_catalog: IcebergCatalog, incremental_load_settings: IncrementalLoad):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.destination_catalog = destination_catalog
    self.incremental_load_settings = incremental_load_settings
    self.full_load_table_recreate_data_migration = FullLoadStrategy(destination_catalog)

  def load_data(self, proxy_table, destination_table):
    logger.debug("logger proxy_table: %s, destination_table: %s", proxy_table, destination_table)

    if not self.destination_catalog.is_table_exists(destination_table):

      if self.incremental_load_settings.start_date:
        self.destination_catalog.create_table_like(proxy_table, destination_table)
      else:
        self.logger.info("Table doesn't exists. Doing full dump")
        self.full_load_table_recreate_data_migration.load_data(proxy_table, destination_table)
        return

    primary_key = self.incremental_load_settings.primary_key
    partition_column = self.incremental_load_settings.partition_column
    start_date = self.__get_start_date(destination_table)
    end_date = self.incremental_load_settings.end_date

    proxy_table_columns = self.destination_catalog.get_table_column_names(proxy_table)

    insert_columns = ",".join(proxy_table_columns)
    insert_values = ",".join(f"src.{c}" for c in proxy_table_columns)

    update_columns = ",".join([f"trg.{c}=src.{c}"
                               for c in proxy_table_columns if c != primary_key])
    merge_query = f"""
                MERGE INTO {destination_table} trg
                USING (SELECT * FROM {proxy_table} 
                        WHERE {partition_column} >= '{start_date}' and {partition_column} <= '{end_date}') src
                ON (src.{primary_key} = trg.{primary_key})
                WHEN MATCHED THEN 
                    UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN 
                    INSERT ({insert_columns}) VALUES({insert_values})
            """

    result = self.destination_catalog.run_query(merge_query)

    logger.debug("logger finished! result: %s", result)

  def __get_start_date(self, destination_table: str):
    return self.incremental_load_settings.start_date or self.destination_catalog.get_single_value(
      query=f"select max({self.incremental_load_settings.partition_column}) from {destination_table}"
    )


class FullLoadStrategy(DataLoadStrategy):
  def __init__(self, destination_catalog: IcebergCatalog):
    self.destination_catalog = destination_catalog

  def load_data(self, proxy_table, destination_table):
    logger.debug("Sync started..., proxy_table: %s, destination_table: %s",
                 proxy_table, destination_table)

    result = self.destination_catalog.run_query(
      f"CREATE OR REPLACE TABLE {destination_table} SELECT * FROM {proxy_table}")

    logger.debug("logger finished!")

    return result
