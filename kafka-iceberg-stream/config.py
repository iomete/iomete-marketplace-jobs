import os
from dataclasses import dataclass

from pyhocon import ConfigFactory


@dataclass
class TriggerConfig:
    processing_time: str = None
    once: bool = False

    def __post_init__(self):
        if not self.once and not self.processing_time:
            raise ValueError("either processing_time or once must be set")

        # both cannot be set
        if self.once and self.processing_time:
            raise ValueError("both processing_time and once cannot be set")


@dataclass
class KafkaConfig:
    options: dict[str, str]
    trigger: TriggerConfig
    schema_registry_url: str
    serialization_format: str
    checkpoint_location: str


@dataclass
class DestinationConfig:
    database: str
    table: str

    @property
    def full_table_name(self):
        return f"{self.database}.{self.table}"


@dataclass
class ApplicationConfig:
    kafka: KafkaConfig
    destination: DestinationConfig


def get_config(application_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_path)

    serialization_format = config.get_string("kafka.serialization_format", default="json")
    if serialization_format not in ["json"]:
        raise ValueError(f"serialization format {serialization_format} is not supported. Supported formats are: json")

    trigger_config = TriggerConfig(
        processing_time=config.get("kafka.trigger.processing_time", default=None),
        once=config.get_bool("kafka.trigger.once", default=False)
    )

    options = config["kafka.options"].as_plain_ordered_dict()

    kafka = KafkaConfig(
        options=options,
        trigger=trigger_config,
        serialization_format=serialization_format,
        schema_registry_url=config.get_string("kafka.schema_registry_url", default="http://127.0.0.1:8081"),
        checkpoint_location=config["kafka.checkpoint_location"]
    )

    return ApplicationConfig(
        kafka=kafka,
        destination=DestinationConfig(
            database=config.get_string("destination.database"),
            table=config.get_string("destination.table")
        )
    )
