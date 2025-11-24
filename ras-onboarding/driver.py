"""Spark job driver for RAS onboarding migration."""

from pyspark.sql import SparkSession

from ras_onboarding.common.config import get_config
from ras_onboarding.main import start_job

config = get_config("/etc/configs/application.conf")

# Dynamic app name based on migration type
migration_type = config.get("migration", {}).get("migration_type", "asset")
app_name = f"RAS Onboarding Migration - {migration_type.capitalize()}"

spark = SparkSession.builder.appName(app_name).getOrCreate()

start_job(spark, config)