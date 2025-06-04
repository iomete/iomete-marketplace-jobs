import pytest

from file_streaming.sync.file_sync import FileSync
from file_streaming.utils.stream_utils import load
from tests.integration._spark_session import get_spark_session
from file_streaming.utils.config import *
from file_streaming.utils.exceptions import IllegalArgumentException

checkpoint = "checkpointLocation"


@pytest.fixture
def spark_session():
    return get_spark_session()


@pytest.fixture
def app_config():
    file = FileConfig(
        format='csv',
        path='{}/resources/'.format(os.path.dirname(__file__)),
        header=True,
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
        checkpoint_location=checkpoint
    )


def test_csv_sync_with_schema_inference(spark_session, app_config):
    app_config.source.file.path += 'csv/'
    app_config.destination.table = 'test_csv_schema_inference'
    spark_session.conf.set('spark.sql.streaming.schemaInference', 'true')

    sync(spark_session, app_config)

    assert_csv_sync(spark_session, app_config)


def test_csv_sync_with_schema(spark_session, app_config):
    app_config.source.file.path += 'csv/'
    app_config.destination.table = 'test_csv_schema'

    schema = [
        {'name': 'id', 'type': 'string'},
        {'name': 'user_id', 'type': 'int'},
        {'name': 'order_date', 'type': 'timestamp'},
        {'name': 'status', 'type': 'string'}
    ]

    app_config.source.schema = schema_from_config(schema)

    sync(spark_session, app_config)

    assert_csv_sync(spark_session, app_config)


def assert_csv_sync(spark_session, app_config):
    query_result = spark_session.sql("select * from " + app_config.destination.full_table_name)

    assert query_result.columns == ['id', 'user_id', 'order_date', 'status']
    assert query_result.count() == 99


def test_json_sync_with_schema_inference(spark_session, app_config):
    app_config.source.file.path += 'json/'
    app_config.source.file.format = 'json'
    app_config.destination.table = 'test_json_schema_inference'
    spark_session.conf.set('spark.sql.streaming.schemaInference', 'true')

    file_sync = FileSync(spark_session, app_config)
    with pytest.raises(IllegalArgumentException, match='Schema must be defined.'):
        dataframe = load(spark_session, app_config)


def test_json_sync_with_schema(spark_session, app_config):
    app_config.source.file.path += 'json/'
    app_config.source.file.format = 'json'
    app_config.destination.table = 'test_json_schema'

    schema = [
        {'name': 'channel', 'type': 'string'},
        {'name': 'event', 'type': 'string'},
        {'name': 'context', 'type': 'struct', 'properties': [
            {'name': 'library', 'type': 'struct', 'properties': [
                {'name': 'name', 'type': 'string'},
                {'name': 'version', 'type': 'string'}
            ]}
        ]},
        {'name': 'receivedAt', 'type': 'timestamp'}
    ]

    app_config.source.schema = schema_from_config(schema)

    sync(spark_session, app_config)

    query_result = spark_session.sql("select * from " + app_config.destination.full_table_name)

    assert query_result.columns == ['channel', 'event', 'context', 'receivedAt']
    assert query_result.count() == 5


def test_s3_sqs_sync_without_schema(spark_session, app_config):
    app_config.source.file.format = 'json'
    app_config.destination.table = 'test_s3_sqs_without_schema'
    spark_session.conf.set('spark.sql.streaming.schemaInference', 'true')

    file_sync = FileSync(spark_session, app_config)
    with pytest.raises(IllegalArgumentException, match='Schema must be defined.'):
        dataframe = load(spark_session, app_config)


def test_sqs_with_schema(spark_session, app_config):
    app_config.source.file.format = 'json'
    app_config.destination.table = 'test_s3_sqs_schema'
    app_config.source.queue = QueueConfig(URL=os.environ.get('S3_SQS_URL'),
                                          fetch_interval_seconds=1,
                                          log_polling_wait_time_seconds=15)

    schema = [
        {'name': 'channel', 'type': 'string'},
        {'name': 'event', 'type': 'string'},
        {'name': 'context', 'type': 'struct', 'properties': [
            {'name': 'library', 'type': 'struct', 'properties': [
                {'name': 'name', 'type': 'string'},
                {'name': 'version', 'type': 'string'}
            ]}
        ]},
        {'name': 'receivedAt', 'type': 'timestamp'}
    ]

    app_config.source.schema = schema_from_config(schema)

    sync(spark_session, app_config)

    query_result = spark_session.sql("select * from " + app_config.destination.full_table_name)

    assert query_result.columns == ['channel', 'event', 'context', 'receivedAt']
    assert query_result.count() == 5


def sync(spark_session, app_config):
    file_sync = FileSync(spark_session, app_config)
    dataframe = load(spark_session, app_config)

    dataframe.writeStream \
        .foreachBatch(file_sync.foreach_batch) \
        .trigger(processingTime=app_config.processing_time) \
        .option(checkpoint, app_config.checkpoint_location) \
        .start() \
        .awaitTermination(20)
