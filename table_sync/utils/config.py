import os
import json

from dataclasses import dataclass


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
class SparkClusterConfig:
  endpoint: str
  token: str
  certificate: str


@dataclass
class KafkaOptionConfig:
  bootstrap_servers: str
  subscribe_topic: str


@dataclass
class KafkaConfig:
  options: KafkaOptionConfig
  trigger: TriggerConfig
  checkpoint_location: str


@dataclass
class DestinationConfig:
  catalog: str
  database: str
  table: str
  primary_key: str

  @property
  def full_table_name(self):
    return f"{self.catalog}.{self.database}.{self.table}"


@dataclass
class ApplicationConfig:
  spark_cluster: SparkClusterConfig
  kafka: KafkaConfig
  destination: DestinationConfig


def get_config() -> ApplicationConfig:
  application_conf = os.getenv("APPLICATION_CONF")

  if os.getenv("ENV") == "local":
    with open("app_conf.json", 'r') as file:
      return _parse_config(json.load(file))

  if application_conf:
    return _parse_config(json.loads(application_conf))

  with open('/etc/configs/app_conf.json', 'r') as file:
    return _parse_config(json.load(file))


def _parse_config(config) -> ApplicationConfig:
  trigger_config = TriggerConfig(
    processing_time=config["kafka"]["trigger"].get('processing_time'),
    once=config["kafka"]["trigger"].get('once', False)
  )

  kafka_options = KafkaOptionConfig(
    bootstrap_servers=config["kafka"]["options"]["bootstrap_servers"],
    subscribe_topic=config["kafka"]["options"]["subscribe_topic"]
  )

  if not config.get("spark_cluster"):
    spark_cluster = None
  else:
    spark_cluster = SparkClusterConfig(
      endpoint=config["spark_cluster"]["endpoint"],
      token=config["spark_cluster"]["token"],
      certificate=config["spark_cluster"].get("certificate")
    )

  kafka = KafkaConfig(
    options=kafka_options,
    trigger=trigger_config,
    checkpoint_location=config["kafka"]["checkpoint_location"]
  )

  return ApplicationConfig(
    spark_cluster=spark_cluster,
    kafka=kafka,
    destination=DestinationConfig(
      catalog=config["destination"]["catalog"],
      database=config["destination"]["database"],
      table=config["destination"]["table"],
      primary_key=config["destination"]["primary_key"]
    )
  )
