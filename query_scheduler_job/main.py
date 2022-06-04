"""Main module."""
import logging
from typing import List

from pyspark.sql import SparkSession

from query_scheduler_job.iomete_logger import init_logger
from utils import timer

logger = logging.getLogger(__name__)


class QueryRunner:
    def __init__(self, spark, queries: List[str]):
        self.spark = spark
        self.queries = queries

    def run_all(self):
        for query in self.queries:
            self.run(query)

    @timer(logger)
    def run(self, query):
        self.spark.sql(query)


def start_job(spark: SparkSession, queries):
    init_logger()
    logger.info("Query runner started...")

    query_runner = QueryRunner(spark, queries)

    query_runner.run_all()

    logger.info("Query runner completed!")
