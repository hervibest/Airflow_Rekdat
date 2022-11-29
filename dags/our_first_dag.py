from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args ={
    'owner': 'Ariq',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'firstDag',
    default_args=default_args,
    description = 'first dag',
    start_date = datetime(2021, 7, 29, 2),
    schedule_interval = '@daily'

) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command="echo hello world"
    )

    task1