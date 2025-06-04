import logging

from pyspark.sql import SparkSession

from file_streaming.utils.config import ApplicationConfig
from file_streaming.utils.stream_utils import load

logger = logging.getLogger(__name__)

checkpoint = "checkpointLocation"


class FileSync:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config

    def sync(self):
        dataframe = load(self.spark, self.config)

        dataframe.writeStream \
            .foreachBatch(self.foreach_batch) \
            .trigger(processingTime=self.config.processing_time) \
            .option(checkpoint, self.config.checkpoint_location) \
            .start() \
            .awaitTermination()

    def foreach_batch(self, df, epoch_id):
        logger.debug(f"[epoch_id={epoch_id}], [table={self.config.destination.full_table_name}]")
        try:
            df.write.saveAsTable(
                name=self.config.destination.full_table_name,
                format='iceberg',
                mode='append',
                partitionBy=self.config.destination.partition_by
            )
        except Exception as e:
            logger.error(f"error stream processing [table={self.config.destination.table}]")
            logger.error(e)
