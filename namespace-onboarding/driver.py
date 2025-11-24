"""Spark job driver for namespace onboarding migration."""

from pyspark.sql import SparkSession

from namespace_onboarding.config import get_config
from namespace_onboarding.main import start_job

config = get_config("/etc/configs/application.conf")

spark = SparkSession.builder.appName("Namespace Onboarding Migration").getOrCreate()

start_job(spark, config)
