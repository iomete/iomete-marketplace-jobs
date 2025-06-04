"""Main module."""

from pyspark.sql import SparkSession

from sync.file_sync import FileSync
from utils.iomete_logger import init_logger


def start_job(spark: SparkSession, config):
    init_logger()
    job = FileSync(spark, config)
    job.sync()

