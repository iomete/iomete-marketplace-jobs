#!/usr/bin/env python

"""Tests for `query_scheduler_job` package."""
from query_scheduler_job.config import get_config
from query_scheduler_job.main import start_job
from tests._spark_session import get_spark_session


def test_query_scheduler_job():
    # create test spark instance
    test_config = get_config("application.conf")
    spark = get_spark_session()

    # start job
    start_job(spark, test_config)

    # check result
    df = spark.sql("SELECT * FROM example.dept_manager;")
    df.printSchema()

    assert df.count() == 24


if __name__ == '__main__':
    test_query_scheduler_job()