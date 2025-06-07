from iomete_sdk.spark.spark_job import SparkJobApiClient


spark_client = SparkJobApiClient(host="dev.iomete.cloud", api_key="asdfa",domain="integration_test_domain")

jobs = spark_client.get_jobs()
print(jobs)
