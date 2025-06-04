import logging
from file_streaming.utils.config import ApplicationConfig
from file_streaming.utils.exceptions import IllegalArgumentException
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

SQS_DATA_SOURCE = 's3-sqs'


class StreamOptions:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.config = config
        self.spark = spark

    @property
    def data_source(self) -> str:
        data_source = self.config.source.file.format.lower()
        if self.config.source.queue is not None:
            data_source = SQS_DATA_SOURCE
            logger.info("Loading data from sqs data source.")
        return data_source

    @property
    def queue_url(self) -> str:
        URL = self.config.source.queue.URL
        if not URL:
            raise IllegalArgumentException("Queue URL not specified.")
        return URL

    @property
    def fetch_interval_seconds(self) -> int:
        fetch_interval_seconds = self.config.source.queue.fetch_interval_seconds
        if not fetch_interval_seconds:
            raise IllegalArgumentException("fetch_interval_seconds not specified.")
        return fetch_interval_seconds

    @property
    def log_polling_wait_time_seconds(self) -> int:
        log_polling_wait_time_seconds = self.config.source.queue.log_polling_wait_time_seconds
        if not log_polling_wait_time_seconds:
            raise IllegalArgumentException("log_polling_wait_time_seconds not specified.")
        return log_polling_wait_time_seconds

    @property
    def region(self) -> str:
        return self.config.source.queue.region

    @property
    def file_format(self) -> str:
        file_format = self.config.source.file.format
        if not file_format:
            raise IllegalArgumentException("File format not specified.")
        return file_format.lower()

    @property
    def schema(self):
        schema = self.config.source.schema
        schema_inference = self.spark.conf.get('spark.sql.streaming.schemaInference')

        if schema is None:
            if self.config.source.queue is not None or \
                    self.file_format == 'json':
                raise IllegalArgumentException("Schema must be defined.")
            elif schema_inference == 'false':
                raise IllegalArgumentException("Neither 'schema' nor 'schemaInference' set in config. "
                                               "You should provide schema or set spark config -> "
                                               "'spark.sql.streaming.schemaInference=true' for inference.")
        return schema

    @property
    def path(self) -> str:
        return self.config.source.file.path

    @property
    def header(self) -> bool:
        return self.config.source.file.header

    @property
    def max_files_per_trigger(self) -> str:
        return self.config.source.file.max_files_per_trigger

    @property
    def latest_first(self) -> bool:
        return self.config.source.file.latest_first

    @property
    def max_file_age(self) -> str:
        return self.config.source.file.max_file_age

    # Whether to check new files based on only the filename instead of on the full path.
    #
    # With this set to `true`, the following files would be considered as the same file, because
    # their filenames, "dataset.txt", are the same:
    # - "file:///dataset.txt"
    # - "s3://a/dataset.txt"
    # - "s3a://a/b/c/dataset.txt"
    @property
    def file_name_only(self) -> bool:
        filename_only = self.config.source.file.filename_only
        if filename_only:
            logger.warning("'fileNameOnly' is enabled. Make sure your file names are unique.")
        return filename_only
