"""Main module."""
import logging
from pyspark.sql import SparkSession

from query_scheduler_job.iomete_logger import init_logger
from query_scheduler_job.utils import timeit

logger = logging.getLogger(__name__)


@timeit
def run_query(spark, query):
    spark.sql(query)


def start_job(spark: SparkSession, queries):
    init_logger()
    logger.info("Query Scheduler Job started")

    for query in queries:
        run_query(spark, query)

    logger.info("Job Finished")
