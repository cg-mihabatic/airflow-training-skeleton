import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import PythonOperator

args = {
  'owner': 'Miha',
  'start_date': airflow.utils.dates.days_ago(2),
}

def print_awesome_string (**context):
  print("This is beautiful\n")  

print_awesome_string = PythonOperator(
  task_id="print_awesome_string",
  python_callable=print_awesome_string,
  provide_contxt=True,
  dag=dag,
)
