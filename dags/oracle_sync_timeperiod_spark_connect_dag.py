import json

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

config = {
  "spark_cluster": {
    "endpoint": Variable.get("SPARK_CLUSTER_ENDPOINT"),
    "token": Variable.get("SPARK_CLUSTER_TOKEN")
  },
  "source_connection": {
    "host": "oracle-db.infra.svc.cluster.local",
    "port": 1521,
    "username": Variable.get("ORACLE_USERNAME"),
    "password": Variable.get("ORACLE_PASSWORD")
  },
  "syncs": [
    {
      "source": {
        "database": "FREEPDB1",
        "schema": "iomete_user",
        "tables": ["ORDERS"]
      },
      "destination": {
        "catalog": "test_abhishek_catalog",
        "schema": "marketplace"
      },
      "sync_mode": {
        "type": "incremental_load",
        "primary_key": "ORDER_ID",
        "partition_column": "updated_at",
        "start_date": "2024-01-01",
        "end_date": "2024-01-30"
      }
    }
  ]
}

with DAG(
    'oracle_sync_timeperiod_spark_connect_dag',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:
  task = KubernetesPodOperator(
    namespace='airflow-abhishek',
    image="iomete/iomete-oracle-sync:1.2.0-spark-connect",
    cmds=["python", "driver.py"],
    name="oracle_sync",
    task_id="oracle_sync_task",
    env_vars={"APPLICATION_CONF": json.dumps(config)},
    get_logs=True,
  )

  task
