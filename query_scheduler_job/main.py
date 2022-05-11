"""Main module."""
from pyspark.sql import SparkSession
from query_scheduler_job.utils import timeit
import structlog

logger = structlog.get_logger("Query Scheduler")


@timeit
def run_query(spark, query):
    logger.info(f'Running query: {query}')
    spark.sql(query)


def start_job(spark: SparkSession, queries):
    logger.info("Query Scheduler Job: 0.1.0")

    for query in queries:
        run_query(spark, query)

    logger.info("Job Finished")
