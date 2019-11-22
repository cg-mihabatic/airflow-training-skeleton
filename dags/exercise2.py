import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

args = {
  'owner': 'Miha',
  'start_date': datetime(2019,11,17),
}

dag = DAG(
  dag_id='exercise2',
  default_args=args,
  'schedule_interval': "@daily",
)

def print_date (**context):
  print("Date = " + str(datetime.today()))  

print_execution_date = PythonOperator(
  task_id="print_execution_date",
  python_callable=print_date,
  provide_context=True,
  dag=dag,
)

the_end = DummyOperator(
    task_id='the_end',
    dag=dag,
)

w1 = BashOperator(task_id="wait_5", bash_command="sleep 5", dag=dag,)
w2 = BashOperator(task_id="wait_1", bash_command="sleep 1", dag=dag,)
w3 = BashOperator(task_id="wait_10", bash_command="sleep 10", dag=dag,)

print_execution_date >> [w1, w2, w3]
[w1, w2, w3] >> the_end
