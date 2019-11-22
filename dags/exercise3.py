import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
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
  print("Day = " + execution_date.strftime("%a"))  

def _get_weekday_number (execution_date, **context):
  return execution_date.weekday()

print_execution_day = PythonOperator(
  task_id="print_execution_date",
  python_callable=print_day,
  provide_context=True,
  dag=dag,
)

weekday_to_person = {
  0: "Bob",
  1: "Joe",
  2: "Alice",
  3: "Joe",
  4: "Alice",
  5: "Alice",
  6: "Alice"
}

def _get_user_on_day(**context):
  person = weekday_to_person[_get_weekday_number]
  return f"email_{person}"

branching = BranchPythonOperator(
  task_id="branching", python_callable=_get_user_on_day, provide_context=True, dag=dag
)

end_bash = BashOperator(
  task_id="final_task", 
  bash_command=f"echo {{params.my_param}}", 
  trigger_rule=TriggerRule.ONE_SUCCESS, 
  dag=dag, 
  params={"my_param":"hello_there"},
)

print_execution_day >> branching

email_tasks = []

for user_name in set(weekday_to_person.values()):
  email_task=DummyOperator(task_id=f"email_{user_name}", dag=dag)
  email_tasks.append(email_task)

branching >> email_tasks >> end_bash
