from airflow.sdk import dag, task
#from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from pendulum import datetime
import pandas as pd
import requests
import io

@dag(
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=['heavy_etl_test']
)
def weather_etl_to_databricks():

    @task
    def extract_weather():
        # Fetching 1 month of hourly temperature data for London
        # No API key required!
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "hourly": "temperature_2m"
        }
        response = requests.get(url, params=params)
        data = response.json()
        
        # The data is nested: data['hourly']['time'] and data['hourly']['temperature_2m']
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temp": data["hourly"]["temperature_2m"]
        })
        return df.to_json()
    @task
    def load_to_uc(json_data):
        df = pd.read_json(io.StringIO(json_data))
        # Use the SQL-specific hook
        # Ensure your 'databricks_default' connection has the 'http_path' 
        # of your SQL Warehouse in the "Extra" field!
        db_hook = DatabricksSqlHook(databricks_conn_id='db_default',use_inline_params=True)
        # UC Table path
        table_path = "practice.raw.db_airflow"
        # 1. Now you can use .run() directly for DDL (Data Definition Language)
        db_hook.run(f"CREATE TABLE IF NOT EXISTS {table_path} (time STRING, temp DOUBLE)")
    
        # 2. Insert the rows
        values = [tuple(x) for x in df.values]
        db_hook.insert_rows(table=table_path, rows=values)

    load_to_uc(extract_weather())

weather_etl_to_databricks()
