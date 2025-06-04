from pyspark.sql import SparkSession

import job

spark = SparkSession.builder.appName(job.JOB_NAME + "-test").getOrCreate()


def test_process():
    job.start()
    df = spark.table("default.all_keycloak_db_changes_v1")
    print("count:", df.count())
    df.printSchema()
