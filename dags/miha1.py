import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
  'owner': 'Miha',
  'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='first_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
)

def print_awesome_string (**context):
  print("This is beautiful\n")  

print_awesome_string = PythonOperator(
  task_id="print_awesome_string",
  python_callable=print_awesome_string,
  provide_contxt=True,
  dag=dag,
)
