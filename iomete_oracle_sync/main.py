import os
import json

from pyspark.sql import SparkSession

from iomete_oracle_sync.data_sync.syncer import Syncer
from iomete_oracle_sync.logger.iomete_logger import init_logger


def start_job():
  init_logger()

  application_conf = os.getenv("APPLICATION_CONF")
  env = os.getenv("ENV")

  if env == "local":
    with open('app_conf.json', 'r') as file:
      config = json.load(file)
      spark = SparkSession.builder.appName("JDBC migration").remote(
        config["spark_cluster"]["endpoint"].replace('{access_token}', config["spark_cluster"]["token"])).getOrCreate()
  elif application_conf:
    config = json.loads(application_conf)
    spark = SparkSession.builder.appName("JDBC migration").remote(
      config["spark_cluster"]["endpoint"].replace('{access_token}', config["spark_cluster"]["token"])).getOrCreate()
  else:
    with open('/etc/configs/app_conf.json', 'r') as file:
      config = json.load(file)
      config["source_connection"]["username"] = os.getenv(config["source_connection"]["username"])
      config["source_connection"]["password"] = os.getenv(config["source_connection"]["password"])

      spark = SparkSession.builder.appName("JDBC migration").getOrCreate()

  Syncer(spark, config).run()
