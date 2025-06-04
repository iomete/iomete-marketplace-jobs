import logging
from pyspark.sql import SparkSession, DataFrame

from file_streaming.utils.config import ApplicationConfig
from file_streaming.sync.stream_options import StreamOptions
from file_streaming.utils.exceptions import IllegalArgumentException

logger = logging.getLogger(__name__)


def load(spark: SparkSession, config: ApplicationConfig) -> DataFrame:
    reader = spark.readStream
    options = StreamOptions(spark, config)

    reader.format(options.data_source)

    reader_options = call_options(options, options.data_source)

    reader_options.update(common_options(options))

    for k in reader_options:
        reader.option(k, reader_options[k])

    dataframe = reader.load(schema=options.schema)

    return dataframe


def sqs_options(options: StreamOptions) -> dict:
    return {
        'fileFormat': options.file_format,
        'sqsUrl': options.queue_url,
        'sqsFetchIntervalSeconds': options.fetch_interval_seconds,
        'sqsLongPollingWaitTimeSeconds': options.log_polling_wait_time_seconds,
        'ignoreFileDeletion': 'true',
        'region': options.region
    }


def csv_options(options: StreamOptions) -> dict:
    return {
        'header': options.header,
        'path': options.path,
        'latestFirst': options.latest_first
    }


def json_options(options: StreamOptions) -> dict:
    return {
        'path': options.path,
        'latestFirst': options.latest_first
    }


def common_options(options: StreamOptions) -> dict:
    return {
        'maxFilesPerTrigger': options.max_files_per_trigger,
        'maxFileAge': options.max_files_per_trigger,
        'fileNameOnly': options.file_name_only
    }


options_by_data_source = {
    's3-sqs': sqs_options,
    'csv': csv_options,
    'json': json_options
}


def call_options(options: StreamOptions, func):
    try:
        return options_by_data_source[func](options)
    except IllegalArgumentException as e:
        raise e
