import logging
import os
import re
from dataclasses import dataclass
from pyhocon import ConfigFactory
from pyspark.sql.types import StructType
from file_streaming.utils.schema_utils import schema_from_config

logger = logging.getLogger(__name__)

checkpointLocation = "file_streaming/data/_checkpoints_" + os.getenv("SPARK_INSTANCE_ID")


@dataclass
class QueueConfig:
    URL: str = None
    fetch_interval_seconds: int = None
    log_polling_wait_time_seconds: int = None

    @property
    def region(self) -> str:
        return re.search('sqs.(.*).amazonaws', self.URL).group(1)


@dataclass
class FileConfig:
    format: str
    max_files_per_trigger: str
    max_file_age: str
    latest_first: bool = False
    path: str = None
    header: bool = False
    filename_only: bool = False


@dataclass
class DestinationConfig:
    schema: str
    table: str
    partitions: list = None

    @property
    def full_table_name(self) -> str:
        return "{}.{}".format(self.schema, self.table)

    @property
    def partition_by(self):
        return self.partitions if self.partitions else None


@dataclass
class SourceConfig:
    file: FileConfig
    schema: StructType = None
    queue: QueueConfig = None


@dataclass
class ApplicationConfig:
    source: SourceConfig
    destination: DestinationConfig
    processing_time: str
    checkpoint_location: str


def format_processing_time(interval, unit) -> str:
    return "{} {}".format(interval, unit)


def get_config(application_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_path)

    def parse_queue(queue_config):
        return QueueConfig(
            URL=queue_config['URL'],
            fetch_interval_seconds=queue_config['fetch_interval_seconds'],
            log_polling_wait_time_seconds=queue_config['log_polling_wait_time_seconds']
        )

    def parse_file(file_config):
        return FileConfig(
            format=file_config['format'],
            header=file_config['header'],
            path=file_config['path'],
            max_files_per_trigger=file_config['max_files_per_trigger'],
            latest_first=file_config['latest_first'],
            max_file_age=file_config['max_file_age']
        )

    def parse_destination(db_config):
        return DestinationConfig(
            schema=db_config['schema'],
            table=db_config['table'],
            partitions=db_config['partitions']
        )

    def parse_processing_time(processing_config):
        return format_processing_time(processing_config['interval'],
                                      processing_config['unit'])

    def parse_source(source_config):
        schema = None
        if hasattr(source_config, 'schema'):
            schema = schema_from_config(source_config['schema'])
        queue = None
        if hasattr(source_config, 'queue'):
            queue = parse_queue(source_config['queue'])
        return SourceConfig(
            file=parse_file(source_config['file']),
            schema=schema,
            queue=queue
        )

    return ApplicationConfig(
        source=parse_source(config['source']),
        processing_time=parse_processing_time(config['processing_time']),
        destination=parse_destination(config['destination']),
        checkpoint_location=checkpointLocation
    )
