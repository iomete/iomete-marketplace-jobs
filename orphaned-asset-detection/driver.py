from pyspark.sql import SparkSession

from orphaned_asset_detection.config import get_config
from orphaned_asset_detection.main import start_job

config = get_config()

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

start_job(spark, config)
