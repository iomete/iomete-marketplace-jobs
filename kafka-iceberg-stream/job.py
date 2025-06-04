import os

from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger

from config import get_config

JOB_NAME = "kafka-iceberg-sync"

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)

# read APP_CONFIG_PATH from env variable
config_path = os.environ.get("APP_CONFIG_PATH", "/etc/configs/application.conf")
config = get_config(config_path)


def foreach_batch_sync(df, epoch_id):
    logger.debug(f"foreach_batch_sync processing epoc_id = {epoch_id}, batch size = {df.count()}")
    if df.rdd.isEmpty():
        return

    try:
        df.write.saveAsTable(
            config.destination.full_table_name,
            format='iceberg',
            mode='append'
        )
    except Exception as e:
        logger.error(f"error while saving batch to table {config.destination.full_table_name}", e)


def table_exists_and_has_data():
    from pyspark.sql.utils import AnalysisException
    try:
        rows_count = spark.table(config.destination.full_table_name).count()
        return rows_count > 0
    except AnalysisException:
        return False


def start():
    logger.info(f"kafka sync started for:\n {config.__dict__}")

    starting_offsets = config.kafka.options.get("startingOffsets", None)

    if starting_offsets:
        logger.info(
            f"startingOffsets is present in config: {starting_offsets}. It's recommended not to set it. When not set, "
            f"job decides whether to start from the beginning or from the latest offset.")
    else:
        is_first_run = not table_exists_and_has_data()

        if is_first_run:
            logger.info("Destination table does not exist or empty. It seems like this is the first run. "
                        "Kafka sync will start from the beginning. Next runs will be incremental.")
            config.kafka.options["startingOffsets"] = "earliest"
        else:
            logger.info("Destination table exists and has data. Kafka sync will start from the latest offset.")
            config.kafka.options["startingOffsets"] = "latest"

    df = (
        spark.readStream.format("kafka")
        .options(**config.kafka.options)
        .load()
        .selectExpr(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType",
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value"
        )
    )

    if config.kafka.trigger.once:
        streaming_query = df.writeStream.foreachBatch(foreach_batch_sync).trigger(once=config.kafka.trigger.once)
    else:
        streaming_query = (
            df.writeStream.foreachBatch(foreach_batch_sync)
            .trigger(processingTime=config.kafka.trigger.processing_time)
        )

    (streaming_query
     .option("checkpointLocation", config.kafka.checkpoint_location)
     .start()
     .awaitTermination()
     )


if __name__ == '__main__':
    start()
