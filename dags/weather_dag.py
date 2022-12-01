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
    dag_id='weathers',
    default_args=default_args,
    description='Schedules Tomtom Ingestion',
    schedule_interval="@daily",
    start_date=days_ago(6),
    catchup=True
) as dag:

    t1 = BashOperator(
        task_id='test_to_csv',
        bash_command='python /opt/airflow/dags/weather_ingestion.py --date {{ ds }}'
    )

    t2 = BashOperator(
        task_id='export_weather_to_db',
        bash_command='python /opt/airflow/dags/weather_to_db.py '
                     '--date {{ ds }} --connection %s' % Variable.get("data_dev_connection")
    )

    t2.set_upstream(t1)