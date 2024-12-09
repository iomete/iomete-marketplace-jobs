from airflow import DAG
from airflow.utils.dates import days_ago
from iomete_airflow_plugin.iomete_operator import IometeOperator


with DAG(
    'oracle_sync_spark_job_dag',
    default_args={'owner': 'airflow'},
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:
  task = IometeOperator(
    task_id="iomete-oracle-db-sync-task",
    job_id="387627f0-ea86-47a1-807d-e4fa893aa18b",
    dag=dag,
  )

  task
