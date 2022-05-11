import pathlib
import tempfile

from pyspark.sql import SparkSession

jars = [filepath.absolute().__str__() for filepath in pathlib.Path("docker/jars/").glob('**/*')]

packages = [
    "org.apache.iceberg:iceberg-spark3-runtime:0.13.1",
    "com.amazonaws:aws-java-sdk-bundle:1.11.920",
    "org.apache.hadoop:hadoop-aws:3.2.0",
    "mysql:mysql-connector-java:8.0.20"
]

lakehouse_dir = tempfile.mkdtemp(prefix="iom-lakehouse")

print(f"Lakehouse dir: {lakehouse_dir}")


def get_spark_session():
    spark = SparkSession.builder \
        .appName("Integration Test") \
        .master("local") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", lakehouse_dir) \
        .config("spark.sql.warehouse.dir", lakehouse_dir) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.sources.default", "iceberg") \
        .getOrCreate()

    return spark
