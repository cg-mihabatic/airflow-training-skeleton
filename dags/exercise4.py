import airflow
from airflow.models import DAG
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {
  "owner": "Miha",
  "start_date": datetime(2019,9,20),
}

dag = DAG(
  dag_id="exercise4", 
  default_args=args, 
  schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
  task_id="transfer_data_from_postgres",
  sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
  bucket="airflow-training-data-1983",
  filename="{{ ds }}/properties_{}.json", 
  postgres_conn_id="GoogleCloudSQL-miha",
  dag=dag,
)
