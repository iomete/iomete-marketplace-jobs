import json

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

config = {
  "spark_cluster": {
    "endpoint": Variable.get("SPARK_CLUSTER_ENDPOINT"),
    "token": Variable.get("SPARK_CLUSTER_TOKEN"),
    "certificate": Variable.get("SPARK_CLUSTER_CERTIFICATE")
  },
  "source_connection": {
    "host": "192.168.106.16",
    "port": 1521,
    "username": Variable.get("ORACLE_USERNAME"),
    "password": Variable.get("ORACLE_PASSWORD")
  },
  "syncs": [
    {
      "source": {
        "database": "FCJPREPROD",
        "schema": "FCJLIVE",
        "tables": [
          "ACTB_HISTORY"
        ]
      },
      "destination": {
        "catalog": "bronze",
        "schema": "flex"
      },
      "sync_mode": {
        "type": "incremental_load",
        "primary_key": "AC_ENTRY_SR_NO",
        "partition_column": "TRN_DT",
        "start_date_offset_days": 5
      }
    }
  ]
}

with DAG(
    'oracle_sync_last_5_days_spark_connect_dag',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:
  task = KubernetesPodOperator(
    namespace='iomete-system',
    image="iomete/iomete-oracle-sync:1.2.1-spark-connect",
    cmds=["python", "driver.py"],
    name="oracle_sync",
    task_id="oracle_sync_task",
    env_vars={"APPLICATION_CONF": json.dumps(config)},
    get_logs=True,
  )

  task
