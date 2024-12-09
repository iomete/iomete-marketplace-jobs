# IOMETE: Oracle Sync (DB Replication)

Quickly move your Oracle tables to IOMETE Lakehouse using either Spark Connet Cluster or Spark Jobs that is easy to configure. All you need to do is provide the settings, and the job will handle the rest. Choose how you want to transfer your data: either a full load or in smaller increments. Scroll down for more details on these sync options.

## Development

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -r requirements/spark_connect_requirements.txt
export ENV=local
```

**Create application configuration file at the project root** - `./app_conf.json`
```json
{
  "spark_cluster": {
    "endpoint": "iomete_spark_cluster_endpoint",
    "token": "iomete_token"
  },
  "source_connection": {
    "host": "hostname",
    "port": 1521,
    "username": "username",
    "password": "password"
  },
  "syncs": [
    {
      "source": {
        "database": "database_name",
        "schema": "schema_name",
        "tables": [
          "TABLE_NAME"
        ]
      },
      "destination": {
        "catalog": "catalog_name",
        "schema": "database_name"
      },
      "sync_mode": {
        "type": "full_load/incremental_load",
        "primary_key": "primary_key_column_name",
        "partition_column": "partition_column_name",
        "start_date_offset_days": 5
      }
    }
  ]
}
```
More info on sync modes:
- You can choose either `full_load` or `incremental_load`.
- With `full_load` mode, the application will sync the entire data from the source table.
- If you choose `incremental_load`, you need to provide the `primary_key` and `partition_column` to sync the data incrementally.
- With `incremental_load` mode, the application will sync the data which is not present in destination table.
- You can also provide `start_date_offset_days` to sync data from the past days.
- You can also provide the `start_date` and `end_date` to sync the data within a specific date range.


**Run application**

```shell
python driver.py
```