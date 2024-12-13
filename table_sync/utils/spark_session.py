APP_NAME = "JDBC migration"

def get_custom_spark_connect_session(sc_url: str, certificate: str):
  from .certificate_handler import CustomChannelBuilder
  from pyspark.sql.connect.session import SparkSession

  return SparkSession.builder.appName(APP_NAME).channelBuilder(CustomChannelBuilder(sc_url, certificate)).getOrCreate()


def get_spark_connect_session(sc_url: str):
  from pyspark.sql import SparkSession

  return SparkSession.builder.appName(APP_NAME).remote(sc_url).getOrCreate()


def get_spark_session():
  from pyspark.sql import SparkSession

  return SparkSession.builder.appName(APP_NAME).getOrCreate()

def get_spark_session_local():
  from pyspark.sql import SparkSession

  jar_dependencies = [
    # For Kafka Streaming
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",

    # For Iceberg
    "org.apache.iceberg:iceberg-spark-extensions-3.5_2.12:1.7.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.7.0",
    "software.amazon.awssdk:s3:2.29.23",
  ]

  packages = ",".join(jar_dependencies)

  return SparkSession.builder \
    .appName(APP_NAME) \
    .master("local") \
    .config("spark.executor.extraJavaOptions", "-Dorg.slf4j.simpleLogger.defaultLogLevel=debug") \
    .config("spark.driver.extraJavaOptions", "-Dorg.slf4j.simpleLogger.defaultLogLevel=debug") \
    .config("spark.jars.packages", packages) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()