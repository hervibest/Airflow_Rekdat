from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='get_weathers',
    default_args=default_args,
    description='Schedules Get Weather From Database',
    schedule_interval="@daily",
    start_date=days_ago(6),
    catchup=True
) as dag:

    t1 = BashOperator(
        task_id='get_data_weather',
        bash_command='python /opt/airflow/dags/get_data_weather.py --date {{ ds }}'

    )
    t1