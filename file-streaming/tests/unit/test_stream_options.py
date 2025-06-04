import pytest
from pyspark.sql import SparkSession
from file_streaming.sync.stream_options import StreamOptions
from file_streaming.utils.exceptions import IllegalArgumentException
from file_streaming.utils.config import *


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def app_config():
    file = FileConfig(
        format='csv',
        max_files_per_trigger='1',
        max_file_age='7d'
    )
    return ApplicationConfig(
        source=SourceConfig(file=file),
        destination=DestinationConfig(
            schema='default',
            table='test'
        ),
        processing_time='5 seconds',
        checkpoint_location='checkpointLocation'
    )


def test_data_source_without_sqs(spark_session, app_config):
    options = StreamOptions(spark_session, app_config)
    assert options.data_source == app_config.source.file.format


def test_data_source_with_sqs(spark_session, app_config):
    app_config.source.queue = QueueConfig(URL="URL", fetch_interval_seconds=1, log_polling_wait_time_seconds=10)
    options = StreamOptions(spark_session, app_config)
    assert options.data_source == 's3-sqs'


def test_queue_options_not_specified(spark_session, app_config):
    app_config.source.queue = QueueConfig()
    options = StreamOptions(spark_session, app_config)
    with pytest.raises(IllegalArgumentException, match="Queue URL not specified."):
        url = options.queue_url
    with pytest.raises(IllegalArgumentException, match="fetch_interval_seconds not specified."):
        fetch_interval = options.fetch_interval_seconds
    with pytest.raises(IllegalArgumentException, match="log_polling_wait_time_seconds not specified."):
        wait_time = options.log_polling_wait_time_seconds


def test_schema_and_schema_inference_not_specified_for_standard_data_source(spark_session, app_config):
    options = StreamOptions(spark_session, app_config)
    with pytest.raises(IllegalArgumentException, match="Neither 'schema' nor 'schemaInference' set in config. "
                                                       "You should provide schema or set spark config -> "
                                                       "'spark.sql.streaming.schemaInference=true' for inference."):
        return options.schema


def test_schema_not_specified_for_sqs_data_source(spark_session, app_config):
    app_config.source.queue = QueueConfig(URL="URL", fetch_interval_seconds=1, log_polling_wait_time_seconds=10)
    options = StreamOptions(spark_session, app_config)
    with pytest.raises(IllegalArgumentException, match="Schema must be defined."):
        return options.schema


def test_schema_inference_specified(spark_session, app_config):
    spark_session.conf.set('spark.sql.streaming.schemaInference', 'true')
    options = StreamOptions(spark_session, app_config)
    schema = options.schema
