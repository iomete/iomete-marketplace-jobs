APP_NAME = "JDBC migration"


def get_custom_spark_connect_session(sc_url: str, certificate: str):
  from iomete_oracle_sync.utils.certificate_handler import CustomChannelBuilder
  from pyspark.sql.connect.session import SparkSession

  return SparkSession.builder.appName(APP_NAME).channelBuilder(CustomChannelBuilder(sc_url, certificate)).getOrCreate()


def get_spark_connect_session(sc_url: str):
  from pyspark.sql import SparkSession

  return SparkSession.builder.appName(APP_NAME).remote(sc_url).getOrCreate()


def get_spark_session():
  from pyspark.sql import SparkSession

  return SparkSession.builder.appName(APP_NAME).getOrCreate()
