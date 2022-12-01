
import os
from datetime import datetime, timedelta
today = datetime.today()
yesterday = today - timedelta(days=1)

print(yesterday)
test = datetime.strftime(yesterday, '%Y-%m-%d')
print(test)

TOMTOM_API = os.getenv("TOMTOM_API", "https://api.midway.tomtom.com/ranking/liveHourly/IDN_jakarta")
CSV_FILE_DIR = os.getenv("CSV_FILE_DIR", "/opt/airflow/dags/datasets/tomtom")
WEATHER_API = os.getenv("WEATHER_API", f"https://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=01749ef9ac104e669c9133109222811&q=Jakarta&format=json&date=2022-11-22&tp=1")
WEATHER_FILE_DIR = os.getenv("CSV_FILE_DIR", "/opt/airflow/dags/datasets/tomtom")

PSQL_DB = os.getenv("PSQL_DB", "airflow")
PSQL_USER = os.getenv("PSQL_USER", "airflow")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "airflow")
PSQL_PORT = os.getenv("PSQL_PORT", "5432")
PSQL_HOST = os.getenv("PSQL_HOST", "localhost")