from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
   'owner': 'baelanche',
   'start_date': datetime(2024, 7, 2, hour=0, minute=00),
   'email': ['baelanche@gmail.com'],
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}

test_dag = DAG(
   "sample_bash",
   schedule="0 9 * * *", 
   tags=['test'],
   catchup=False,
   default_args=default_args 
)

t1 = BashOperator(
   task_id='print_date',
   bash_command='date',
   dag=test_dag)

t2 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
   dag=test_dag)

t3 = BashOperator(
   task_id='ls',
   bash_command='ls /tmp',
   dag=test_dag)

t1 >> [ t2, t3 ]