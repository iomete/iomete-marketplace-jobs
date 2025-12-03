import os
import sys
from pyspark.sql import SparkSession

from ras_onboarding.common.config import get_config
from ras_onboarding.main import start_job

# Support both asset and namespace migration configs
# Set MIGRATION_TYPE environment variable to choose:
# - "asset" (default) -> uses application.conf
# - "namespace" -> uses application-namespace.conf
migration_type_env = os.getenv("MIGRATION_TYPE", "asset").lower()

if migration_type_env == "namespace":
    config_path = "/etc/configs/application-namespace.conf"
    print("Using namespace migration configuration: application-namespace.conf")
elif migration_type_env == "asset":
    config_path = "/etc/configs/application.conf"
    print("Using asset migration configuration: application.conf")
else:
    print(f"Error: Invalid MIGRATION_TYPE '{migration_type_env}'. Must be 'asset' or 'namespace'")
    sys.exit(1)

# Load configuration
config = get_config(config_path)

# Dynamic app name based on migration type
migration_type = config.get("migration", {}).get("migration_type", "asset")
app_name = f"RAS Onboarding Migration - {migration_type.capitalize()}"

spark = SparkSession.builder.appName(app_name).getOrCreate()

start_job(spark, config)