"""from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

DATABRICKS_HOST = "https://dbc-30a0775e-ff62.cloud.databricks.com"

def trigger_databricks():
    url = f"{DATABRICKS_HOST}/api/2.0/jobs/run-now"

    payload = {
        "job_id": 818197214782309   # you'll create this next
    }

    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    response = requests.post(url, json=payload, headers=headers)

    print(response.json())
    print("STATUS:", response.status_code)
    print("RESPONSE:", response.text)

with DAG(
    dag_id="databricks_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
):

    run_dbx = PythonOperator(
        task_id="trigger_databricks",
        python_callable=trigger_databricks
    )


    """


from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from pendulum import datetime

# 1. Use the @dag decorator (Modern TaskFlow API)
@dag(
    dag_id="databricks_modern_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['databricks', 'etl']
)
def databricks_workflow():

    # 2. If you need to do pre-processing in Python before triggering the job
    @task
    def prepare_metadata():
        print("Preparing metadata or checking API status before triggering Databricks...")
        return {"status": "ready"}

    # 3. Use the dedicated Operator for the actual job trigger
    # This is better than a Hook inside a Python task because it handles 
    # the 'Running' state and provides logs automatically.
    run_now_task = DatabricksRunNowOperator(
        task_id='run_existing_databricks_job',
        databricks_conn_id='db_default',
        job_id=818197214782309,
        # notebook_params={'run_date': '2026-05-04'} 
    )

    # 4. Set the dependency flow
    prepare_metadata() >> run_now_task

# 5. Call the function to register the DAG
databricks_workflow()