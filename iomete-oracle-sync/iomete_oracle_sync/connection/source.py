from abc import abstractmethod


class SourceConnection:
  def __init__(self, host: str, port: str, user_name: str, user_pass: str):
    self.host = host
    self.port = port
    self.user_name = user_name
    self.user_pass = user_pass

  @abstractmethod
  def url(self, database):
    pass

  @property
  def driver(self):
    return None

  @abstractmethod
  def get_spark_jdbc_tables_info_create_query(self):
    pass

  @abstractmethod
  def get_spark_jdbc_table_create_query(self, database: str, source_table: str, proxy_table: str, schema: str = None):
    pass

  @abstractmethod
  def get_table_info_query(self, schema: str):
    pass

  @abstractmethod
  def drop_table_info_query(self):
    pass


class OracleConnection(SourceConnection):
  def __init__(self, host: str, port: str, user_name: str, user_pass: str):
    super().__init__(host, port, user_name, user_pass)
    self.info_table_name = "ALL_TABLES"

  def url(self, database):
    return f'jdbc:oracle:thin:@{self.host}:{self.port}/{database}'

  @property
  def driver(self):
    return 'oracle.jdbc.OracleDriver'

  def get_spark_jdbc_tables_info_create_query(self):
    return self.get_spark_jdbc_table_create_query(
      source_table=self.info_table_name,
      proxy_table=self.info_table_name
    )

  def get_spark_jdbc_table_create_query(self, database: str, source_table: str, proxy_table: str, schema: str = None):
    db_table_name = f'{schema}."{source_table}"' if schema else source_table

    return f"""
            CREATE OR REPLACE TABLE {proxy_table}
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              url '{self.url(database=database)}',
              dbtable '{db_table_name}',
              user '{self.user_name}',
              password '{self.user_pass}',
              driver '{self.driver}',
              oracle.jdbc.mapDateToTimestamp 'false'
            )
        """

  def get_table_info_query(self, schema: str):
    return f"SELECT * FROM {self.info_table_name} where owner = '{schema.upper()}'"

  def drop_table_info_query(self):
    return f"DROP TABLE IF EXISTS {self.info_table_name}"

  def __str__(self):
    return f"Oracle Connection(host: '{self.host}')"
