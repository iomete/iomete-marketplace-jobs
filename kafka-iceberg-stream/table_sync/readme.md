# IOMETE: Spark Streaming Job

Pipeline data from your tables to IOMETE Lakehouse using Spark Streaming Job.

## Configuration

- Add maven package to IOMETE Job - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
- Add config in config maps at path `/etc/configs/app_conf.json`
    ```json
    {
      "kafka": {
        "options": {
          "bootstrap_servers": "host_name:9092",
          "subscribe_topic": "topic_name"
        },
        "trigger": {
          "processing_time": "10 seconds"
        },
        "checkpoint_location": "path/to/checkpoint"
      },
      "destination": {
        "catalog": "catalog_name",
        "database": "database_name",
        "table": "table_name",
        "primary_key": "primary_key_column_name"
      }
    }
    ```

## Update Image

**Spark Streaming Job Image**

```shell
docker buildx build --platform linux/amd64,linux/arm64 --push -f table_sync/infra/Dockerfile -t iomete/iomete-kafka-sync:1.0.1 .
```

