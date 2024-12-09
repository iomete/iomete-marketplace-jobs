import logging
import re

logger = logging.getLogger(__name__)


class IcebergCatalog:
  def __init__(self, spark, catalog_name: str, db_name: str):
    self.spark = spark
    self.catalog_name = catalog_name
    self.db_name = db_name

  def get_full_table_name(self, table_name: str) -> str:
    return f"{self.catalog_name}.{self.db_name}.{table_name}"

  def is_table_exists(self, table_name: str):
    df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.db_name}")
    table_part = table_name.split(".")[-1]

    return df.where(df.tableName == table_part).count() > 0

  def get_table_column_names(self, table_name: str):
    return self.spark.table(table_name).schema.names

  def run_query(self, query):
    logger.debug("Executing query: %s", self.__safe_string_for_log(query))

    return self.spark.sql(query).collect()

  def create_database(self):
    self.run_query(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.db_name}")

  def get_single_value(self, query):
    result = self.run_query(query)

    if result and len(result) > 0:
      return result[0][0]

    return None

  def create_table_like(self, source_table, destination_table):
    self.run_query(f"CREATE TABLE {destination_table} AS SELECT * FROM {source_table} WHERE 1=0")

  @staticmethod
  def __safe_string_for_log(log_text: str):
    log_text = re.sub(r"""(password\s')(.*)(',)""", r"\1*****\3", log_text)
    return log_text
