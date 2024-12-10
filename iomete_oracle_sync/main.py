import os
import json

from iomete_oracle_sync.data_sync.syncer import Syncer
from iomete_oracle_sync.logger.iomete_logger import init_logger
from iomete_oracle_sync.utils.config_utils import get_config
from iomete_oracle_sync.utils.spark_session import get_custom_spark_connect_session, get_spark_connect_session, \
  get_spark_session


def start_job():
  init_logger()

  config = get_config()

  if os.getenv("ENV") == "local" or os.getenv("APPLICATION_CONF"):
    spark_cluster_conf = config["spark_cluster"]
    sc_url = spark_cluster_conf["endpoint"].replace('{access_token}', spark_cluster_conf["token"])

    if config["spark_cluster"].get("certificate"):
      spark = get_custom_spark_connect_session(sc_url, spark_cluster_conf.get("certificate"))
    else:
      spark = get_spark_connect_session(sc_url)
  else:
    with open('/etc/configs/app_conf.json', 'r') as file:
      config = json.load(file)
      config["source_connection"]["username"] = os.getenv(config["source_connection"]["username"])
      config["source_connection"]["password"] = os.getenv(config["source_connection"]["password"])

      spark = get_spark_session()

  Syncer(spark, config).run()
