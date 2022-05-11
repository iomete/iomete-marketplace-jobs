from pyspark.sql import SparkSession

from query_scheduler_job.config import get_config
from query_scheduler_job.main import start_job

spark = SparkSession.builder \
    .appName("query_scheduler_job") \
    .getOrCreate()

production_config = get_config("/etc/configs/application.conf")

start_job(spark, production_config)
