import os
import random

from attrs import define, validators, field
from pyhocon import ConfigFactory
from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger

JOB_NAME = "kinesis_streaming"

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)


def mask_value(value, mask_char="*", mask_length=15):
    if value is None:
        return None

    if len(value) <= mask_length:
        return mask_char * len(value)

    return value[:mask_length] + mask_char * (len(value) - mask_length)


def non_empty_string(instance, attribute, value):
    if not value:
        raise ValueError(f"{attribute.name} cannot be empty")


@define
class KinesisConfig:
    stream_name: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))
    region: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))
    starting_position: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))
    aws_access_key_id: str = field(repr=lambda value: mask_value(value))
    aws_secret_access_key: str = field(repr=lambda value: mask_value(value))

    @starting_position.validator
    def _validate_starting_position(self, attribute, value):
        possible_values_message = "Possible values are: 'latest' or 'trim_horizon'"

        if not value:
            raise ValueError(f"starting_position must be set. {possible_values_message}")

        if value not in ["latest", "trim_horizon"]:
            raise ValueError(f"starting_position is invalid. {possible_values_message}")


@define
class DestinationConfig:
    table_name: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))
    batch_time: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))
    checkpoint_location: str = field(validator=validators.and_(validators.instance_of(str), non_empty_string))


@define
class ApplicationConfig:
    kinesis_config: KinesisConfig
    destination_config: DestinationConfig


def read_application_config(logger) -> ApplicationConfig:
    application_config_path = os.environ.get("APPLICATION_CONFIG_FILE")
    if application_config_path is None:
        raise ValueError("APPLICATION_CONFIG_FILE environment variable is not set")

    logger.info(f"Application config path: {application_config_path}")

    config = ConfigFactory.parse_file(application_config_path)

    application_config = ApplicationConfig(
        kinesis_config=KinesisConfig(
            stream_name=config.get_string("kinesis.stream_name"),
            region=config.get_string("kinesis.region"),
            starting_position=config.get_string("kinesis.starting_position", default="latest"),
            aws_access_key_id=config.get_string("kinesis.aws_access_key_id"),
            aws_secret_access_key=config.get_string("kinesis.aws_secret_access_key")
        ),
        destination_config=DestinationConfig(
            table_name=config.get_string("destination.table_name"),
            batch_time=config.get_string("destination.batch_time"),
            checkpoint_location=config.get_string("destination.checkpoint_location")
        )
    )

    logger.info(f"Application config: {application_config}")

    return application_config


application_config = read_application_config(logger)

transformation_sql = """
SELECT cast(data as string),
              streamName as metadata_stream_name,
              partitionKey as metadata_partition_key,
              sequenceNumber as metadata_sequence_number,
              approximateArrivalTimestamp as metadata_approximate_arrival_timestamp
          FROM source_stream
"""


def process():
    logger.info(f"Starting Kinesis streaming process...")

    kinesis_config = application_config.kinesis_config

    # Read data from the Kinesis stream
    df = (
        spark
        .readStream
        .format("kinesis")
        .option("streamName", kinesis_config.stream_name)
        .option("region", kinesis_config.region)
        .option("endpointUrl", f"https://kinesis.{kinesis_config.region}.amazonaws.com")
        .option("awsAccessKeyId", kinesis_config.aws_access_key_id)
        .option("awsSecretKey", kinesis_config.aws_secret_access_key)
        .option("awsUseInstanceProfile", 'false')
        # Possible values are "latest", "trim_horizon", "earliest" or JSON serialized map shardId->KinesisPosition
        # {"shardId":"shardId-000000000001","iteratorType":"AFTER_SEQUENCE_NUMBER","iteratorPosition":"49638253970074927406522936340125600836039637694182064146"}
        .option("startingPosition", kinesis_config.starting_position.lower())
        .option("format", "json")
        .option("inferSchema", "true")
        .load()
    )

    df.createOrReplaceTempView("source_stream")
    transformed_df = spark.sql(transformation_sql)

    logger.info(f"Stream schema:")
    transformed_df.printSchema()

    destination_config = application_config.destination_config

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {destination_config.table_name} (
        data string,
        metadata_stream_name string,
        metadata_partition_key string,
        metadata_sequence_number string,
        metadata_approximate_arrival_timestamp timestamp)
        TBLPROPERTIES (
            'history.expire.max-snapshot-age-ms'='10000',
            'commit.manifest.min-count-to-merge'='2',
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='2'
        )
        PARTITIONED BY (days(metadata_approximate_arrival_timestamp))
    """)

    ws = transformed_df.writeStream \
        .trigger(processingTime=destination_config.batch_time) \
        .option("fanout-enabled", "true") \
        .toTable(
            tableName=destination_config.table_name,
            format="iceberg",
            outputMode="append",
            checkpointLocation=destination_config.checkpoint_location
        )

    ws.awaitTermination()


if __name__ == '__main__':
    process()
