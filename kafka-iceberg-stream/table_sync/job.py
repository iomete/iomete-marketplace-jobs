import os

from pyspark.sql.functions import from_json, col

from utils.config import get_config
from utils.config import ApplicationConfig
from utils.spark_session import get_custom_spark_connect_session, get_spark_connect_session, get_spark_session


def _foreach_batch_sync_wrapper(spark, config):
  def foreach_batch_sync(df, epoch_id):
    if df.rdd.isEmpty():
      return

    try:
      print(f"Processing epoch_id = {epoch_id}, batch size = {df.count()}")

      source_table = "kafka_batch_table"
      destination_table = config.destination.full_table_name
      destination_table_columns = spark.table(destination_table).schema.names
      primary_key = config.destination.primary_key

      insert_columns = ",".join(destination_table_columns)
      insert_values = ",".join(f"src.{c}" for c in destination_table_columns)
      update_columns = ",".join([f"trg.{c}=src.{c}"
                                 for c in destination_table_columns if c != primary_key])

      df.createOrReplaceGlobalTempView(source_table)

      merge_query = \
        f"""
           MERGE INTO {destination_table} trg
           USING global_temp.{source_table} src
           ON (src.{primary_key} = trg.{primary_key})
           WHEN MATCHED THEN 
               UPDATE SET {update_columns}
           WHEN NOT MATCHED THEN 
               INSERT ({insert_columns}) VALUES({insert_values})
        """

      print(f"Executing query: {merge_query}")

      spark.sql(merge_query).collect()

      print(f"Batch saved to table {destination_table}")
    except Exception as e:
      print(f"Error while saving batch", e)

  return foreach_batch_sync


def start_job():
  config = get_config()

  if os.getenv("ENV") == "local" or os.getenv("APPLICATION_CONF"):
    spark_cluster_conf = config.spark_cluster
    sc_url = spark_cluster_conf.endpoint.replace('{access_token}', spark_cluster_conf.token)

    if config.spark_cluster.certificate:
      spark = get_custom_spark_connect_session(sc_url, spark_cluster_conf.certificate)
    else:
      spark = get_spark_connect_session(sc_url)
  else:
    with open('/etc/configs/app_conf.json', 'r') as file:
      spark = get_spark_session()

  _run_kafka_pipeline(spark, config)


def _get_table_schema(spark, config: ApplicationConfig):
  return spark.table(config.destination.full_table_name).schema


def _run_kafka_pipeline(spark, config: ApplicationConfig):
  kafka_value_schema = _get_table_schema(spark, config)

  kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config.kafka.options.bootstrap_servers)
    .option("subscribe", config.kafka.options.subscribe_topic)
    .option("startingOffsets", "latest")
    .load()
  )

  parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_value_schema).alias("parsed_value")
  ).select("parsed_value.*")

  if config.kafka.trigger.once:
    streaming_query = parsed_df.writeStream.trigger(once=config.kafka.trigger.once)
  else:
    streaming_query = parsed_df.writeStream.trigger(processingTime=config.kafka.trigger.processing_time)

  (
    streaming_query
    .foreachBatch(_foreach_batch_sync_wrapper(spark, config))
    .start()
    .awaitTermination()
  )


if __name__ == '__main__':
  start_job()
