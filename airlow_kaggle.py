from airflow.decorators import dag, task
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from databricks.sdk import WorkspaceClient
from pendulum import datetime
import os


@dag(
    dag_id="ingest_kaggle_to_uc_volumes",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False
)
def kaggle_to_databricks_volumes():

    @task
    def download_from_kaggle():
        from kaggle.api.kaggle_api_extended import KaggleApi

        # Set Kaggle credentials (In prod, use Airflow Variables/Connections!)
        os.environ['KAGGLE_USERNAME'] = os.getenv("KAGGLE_USERNAME")
        os.environ['KAGGLE_KEY'] = os.getenv("KAGGLE_KEY")
        
        api = KaggleApi()
        api.authenticate()
        
        dataset_slug = "shivamb/netflix-shows"
        download_path = "/tmp/kaggle_data"
        
        # Ensure directory exists
        os.makedirs(download_path, exist_ok=True)
        
        # Download (this handles the zip/csv)
        print(f"Starting download for {dataset_slug}...")
        api.dataset_download_files(dataset_slug, path=download_path, unzip=True)
        
        # Find the actual file name
        file_name = os.listdir(download_path)[0]
        full_path = os.path.join(download_path, file_name)
        
        return {"local_path": full_path, "file_name": file_name}

    @task
    def upload_to_uc_volume(file_info):
        local_file = file_info['local_path']
        remote_file_name = file_info['file_name']
        
        # Unity Catalog Volume Path Structure:
        # /Volumes/<catalog>/<schema>/<volume_name>/<folder>/
        volume_path = f"/Volumes/practice/raw/kaggle_files/{remote_file_name}"
        
        db_hook = DatabricksHook(databricks_conn_id="db_default")
        
        conn = db_hook.get_connection("db_default")
    
        # 2. Manually initialize the WorkspaceClient
        # This bypasses the AttributeError by going straight to the SDK
        w = WorkspaceClient(
        host=conn.host,
        token=conn.password
        )
    
        # 3. Upload the file
        print(f"Uploading {local_file} to {volume_path}...")
        with open(local_file, "rb") as f:
            w.files.upload(volume_path, f)
            
        print(f"Successfully uploaded to Unity Catalog Volume!")
        # Cleanup local storage
        # Cleanup
        if os.path.exists(local_file):
            os.remove(local_file)

        return volume_path
    
    databricks_jobs = DatabricksRunNowOperator(
    task_id='airflow_to_databricks',
    databricks_conn_id='db_default',
    job_id=839970106342159  # Your Job ID from the Databricks UI
)

  
    # Dependency Flow
    file_metadata = download_from_kaggle()
    upload_task = upload_to_uc_volume(file_metadata)

    # Chain the Python tasks to the Databricks Operators
    upload_task >> databricks_jobs


kaggle_to_databricks_volumes()
