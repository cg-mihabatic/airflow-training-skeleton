import airflow
from airflow.models import DAG

args = {
  "owner": "Miha",
  "start_date": datetime.date(2019,09,20),
}
dag = DAG(
  dag_id="exercise4", 
  default_args=args, 
  schedule_interval="0 0 * * *",
)
