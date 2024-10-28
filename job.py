import os
import time

from pyhocon import ConfigFactory
from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger

JOB_NAME = "query-scheduler-job"

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)


def run_query(query):
    logger.info(f"Running query: {query}")
    start_time = time.time()

    # Execute the command query
    val result = spark.sql(query)
    # Show output
    result.show(truncate=False)

    duration = (time.time() - start_time)
    logger.info(f"Query executed in {duration:0.2f} seconds")


def start_job():
    logger.info("Query runner started...")
    application_config_path = os.environ.get("APPLICATION_CONFIG_PATH", "/etc/configs/application.conf")
    queries = ConfigFactory.parse_file(application_config_path)

    for query in queries:
        run_query(query)
    logger.info("Query runner completed!")


if __name__ == '__main__':
    start_job()
