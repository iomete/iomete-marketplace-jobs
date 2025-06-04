import logging

from iomete_oracle_sync.data_sync.conf_parser.data_sync_config import parse_config
from ..utils.time_utils import timer
from ._data_source_syncer import DataSourceSyncer

logger = logging.getLogger(__name__)


class Syncer:
  def __init__(self, spark, config):
    self.config = parse_config(config)
    self.spark = spark

  @timer("Data logger")
  def run(self):
    logger.info(f"Connection={str(self.config.source_connection)}")

    for sync_config in self.config.sync_configs:
      message = (f"Syncing from source: db.schema -> '{sync_config.source.database}.{sync_config.source.schema}' "
                 f"with mode -> {sync_config.sync_mode}")

      data_source_syncer = DataSourceSyncer(self.spark, self.config.source_connection, sync_config)

      timer(message)(data_source_syncer.sync_tables)()
