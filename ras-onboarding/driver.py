import os
from pyspark.sql import SparkSession

from ras_onboarding.common.config import get_config
from ras_onboarding.main import start_job

migration_type_env = os.getenv("MIGRATION_TYPE", "asset").lower()

config = get_config()

migration_type = config.get("migration", {}).get("migration_type", "asset")
app_name = f"RAS Onboarding Migration - {migration_type.capitalize()}"

spark = SparkSession.builder.appName(app_name).getOrCreate()

start_job(spark, config)