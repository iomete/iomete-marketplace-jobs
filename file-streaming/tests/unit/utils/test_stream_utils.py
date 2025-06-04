import pytest
from pyspark.sql import SparkSession
from file_streaming.utils.config import *
from file_streaming.utils.exceptions import IllegalArgumentException
from file_streaming.utils.stream_utils import load
from file_streaming.utils.schema_utils import schema_from_config


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def app_config():
    file = FileConfig(
        format='csv',
        path='{}/resources/{}/'.format(os.path.dirname(__file__), 'csv'),
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


def test_load_csv_without_schema_inference(spark_session, app_config):
    with pytest.raises(IllegalArgumentException, match="Neither 'schema' nor 'schemaInference' set in config. "
                                                       "You should provide schema or set spark config -> "
                                                       "'spark.sql.streaming.schemaInference=true' for inference."):
        dataframe = load(spark_session, app_config)


def test_load_csv_with_defined_schema(spark_session, app_config):
    # id,user_id,order_date,status
    schema = [
        {'name': 'id', 'type': 'string'},
        {'name': 'user_id', 'type': 'string'},
        {'name': 'order_date', 'type': 'timestamp'},
        {'name': 'status', 'type': 'string'}
    ]
    app_config.source.schema = schema_from_config(schema)
    dataframe = load(spark_session, app_config)
