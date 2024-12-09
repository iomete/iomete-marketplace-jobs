import logging

from iomete_oracle_sync.connection.destination import IcebergCatalog
from iomete_oracle_sync.connection.source import SourceConnection
from iomete_oracle_sync.data_sync.sync_strategy.sync_strategy import DataLoadStrategyFactory
from iomete_oracle_sync.data_sync.conf_parser.data_sync_config import SyncConfig
from iomete_oracle_sync.utils.time_utils import table_timer

logger = logging.getLogger(__name__)


class DataSourceSyncer:
  def __init__(self, spark, source_connection: SourceConnection, sync_config: SyncConfig):
    self.spark = spark
    self.source_connection = source_connection
    self.sync_config = sync_config
    self.destination_catalog = IcebergCatalog(spark=spark, catalog_name=sync_config.destination.catalog,
                                              db_name=sync_config.destination.schema)

  @staticmethod
  def __log_tables(tables):
    new_line_tab = "\n\t- "
    log_tables = new_line_tab.join([table for table in tables])
    logger.info(f"Following tables will be synced: {new_line_tab}{log_tables}")

  def sync_tables(self):
    tables = self.sync_config.source.tables

    if self.sync_config.source.include_all_tables:
      tables = self.__get_source_tables()

    excluded_tables = set(self.sync_config.source.excluded_tables or [])

    tables = [table for table in tables if table not in excluded_tables]

    self.__log_tables(tables)

    max_table_name_length = max([len(table) for table in tables])

    for table in tables:
      message = f"[{table: <{max_table_name_length}}]: table sync"
      table_timer(message)(self.__sync_table)(table)
      # self.__sync_table_batches(table)

  def __get_source_tables(self):
    self.destination_catalog.run_query(self.source_connection.get_spark_jdbc_tables_info_create_query())

    source_tables = self.destination_catalog.run_query(
      self.source_connection.get_table_info_query(self.sync_config.source.schema)
    )

    self.destination_catalog.run_query(self.source_connection.drop_table_info_query())

    return [tbl.TABLE_NAME for tbl in source_tables]

  def __sync_table(self, table: str):
    proxy_table = table
    destination_table = self.destination_catalog.get_full_table_name(table)

    self.__create_proxy_table(
      database=self.sync_config.source.database,
      schema=self.sync_config.source.schema,
      source_table=table,
      proxy_table=proxy_table
    )

    data_sync = DataLoadStrategyFactory.instance_for(
      sync_mode=self.sync_config.sync_mode, destination_catalog=self.destination_catalog
    )

    data_sync.load_data(proxy_table, destination_table)

    current_rows_count = self.destination_catalog.get_single_value(f"select count(1) from {destination_table}")

    logger.debug(f"Cleaning up proxy table: {proxy_table}")
    self.destination_catalog.run_query(f"DROP TABLE {proxy_table}")

    return current_rows_count

  def __sync_table_batches(self, table: str):
    df = (
      self.spark.read
      .format("jdbc")
      .option("url", self.source_connection.url(self.sync_config.source.database))
      .option("user", self.source_connection.user_name)
      .option("password", self.source_connection.user_pass)
      .option("dbtable", f"{self.sync_config.source.schema}.{table}")
      .option("driver", self.source_connection.driver)
      .option("partitionColumn", "ORDER_ID")
      .option("lowerBound", 1)
      .option("upperBound", 100000)
      .option("numPartitions", 1000)
      .load()
    )

    (
      df.write
      .format("iceberg")
      .mode("overwrite")
      .save(self.destination_catalog.get_full_table_name(table))
    )

  def __create_proxy_table(self, database: str, schema: str, source_table: str, proxy_table: str):
    self.destination_catalog.run_query(
      self.source_connection.get_spark_jdbc_table_create_query(
        database=database,
        schema=schema,
        source_table=source_table,
        proxy_table=proxy_table
      )
    )
