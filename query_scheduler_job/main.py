"""Main module."""
from pyspark.sql import SparkSession

from query_scheduler_job.iometeLogger import iometeLogger
from query_scheduler_job.utils import timeit
import structlog

logger = iometeLogger("Query Scheduler").get_logger()


@timeit
def run_query(spark, query):
    logger.info(f'Running query: {query}')
    spark.sql(query)


def start_job(spark: SparkSession, queries):
    logger.info("Query Scheduler Job: 0.1.3")

    for query in queries:
        run_query(spark, query)

    logger.info("Job Finished")
