from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger

from job import read_application_config

spark = SparkSession.builder.appName("query-table").getOrCreate()
logger = get_spark_logger(spark=spark)

application_config = read_application_config(logger)
destination_table = application_config.destination_config.table_name

tbl = spark.sql(f"select * from {destination_table} limit 100")

tbl.printSchema()
print("Total rows: ", tbl.count())

tbl.show(truncate=False, n=100)
