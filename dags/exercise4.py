import airflow
from airflow.models import DAG
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow_training.operators import HttpToGcsOperator

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
  filename="land_registry/{{ ds }}/properties_{}.json", 
  postgres_conn_id="GoogleCloudSQL-miha",
  dag=dag,
)

currency = "EUR"

transfer_currency = HttpToGcsOperator( 
  task_id="get_currency_" + currency, 
  method="GET",
  endpoint=f"/history?start_at={{yesterday_ds}}&end_at={{ds}}&symbols={currency}&base=GBP", 
  http_conn_id="airflow-training-currency-http",
  gcs_path="currency/{{ ds }}-" + currency + ".json", 
  gcs_bucket="airflow-training-data-1983",
  dag=dag, 
)

pgsl_to_gcs >> transfer_currency
