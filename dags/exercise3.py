import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

args = {
  'owner': 'Miha',
  "start_date": airflow.utils.dates.days_ago(14),
}

dag = DAG(
  dag_id='exercise3',
  default_args=args,
  schedule_interval="@daily",
)

def print_day (execution_date, **context):
  print("Date = " + execution_date.strftime("%a"))  

def _get_weekday_number (execution_date, **context):
  return execution_date.weekday()

print_execution_day = PythonOperator(
  task_id="print_execution_date",
  python_callable=print_day,
  provide_context=True,
  dag=dag,
)

print_execution_day >> branching

weekday_to_person = {
  0: "Bob",
  1: "Joe",
  2: "Alice",
  3: "Joe",
  4: "Alice",
  5: "Alice",
  6: "Alice"
}

branching = BranchPythonOperator(
  task_id="branching", python_callable=_get_weekday_number, provide_context=True, dag=dag
)

end_bash = BashOperator(
  task_id="final_task", dag=dag,
)

list =[]
for val in weekday_to_person.values(): 
  if val in list: 
    continue 
  else:
    branching >> DummyOperator(task_id=f"email_{val}", dag=dag) >> end_bash
