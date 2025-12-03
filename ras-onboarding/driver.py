from pyspark.sql import SparkSession

from common.config import get_config
from ras_onboarding.main import start_job

config = get_config("/etc/configs/application.conf")

spark = SparkSession.builder.appName("Asset RAS Onboarding Migration").getOrCreate()

start_job(spark, config)