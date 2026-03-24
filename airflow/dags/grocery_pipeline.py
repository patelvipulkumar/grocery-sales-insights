from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.secret_manager import CloudSecretManagerHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from datetime import datetime, timedelta
import os
import subprocess
import json
import pathlib
import requests
from google.auth.transport.requests import Request

PROJECT_ID = os.getenv("GCP_PROJECT", "your-project-id")
RAW_BUCKET = os.getenv("RAW_BUCKET", "your-project-id-grocery-raw")
BQ_DATASET_RAW = "grocery_raw"
BQ_DATASET_ANALYTICS = "grocery_analytics"
KAGGLE_DATASET = "drexibiza/grocery-sales-dataset"


def fetch_kaggle_token() -> str:
    sm = CloudSecretManagerHook()
    secret = sm.get_secret("kaggle-api-token")
    if isinstance(secret, dict):
        return secret.get('payload', {}).get('data', '')
    return secret


def download_kaggle(**context):
    token = fetch_kaggle_token()
    os.makedirs("/tmp/kaggle", exist_ok=True)
    with open("/tmp/kaggle/kaggle.json", "w") as f:
        f.write(json.dumps({"username": "", "key": token}))

    env = os.environ.copy()
    env["KAGGLE_CONFIG_DIR"] = "/tmp/kaggle"

    subprocess.check_call(["kaggle", "datasets", "download", "-d", KAGGLE_DATASET, "-p", "/tmp", "--unzip"], env=env)


def upload_to_gcs(**context):
    for local_file in pathlib.Path("/tmp").glob("*.csv"):
        blob_name = f"raw/{local_file.name}"
        LocalFilesystemToGCSOperator(
            task_id=f"upload_{local_file.stem}",
            src=str(local_file),
            dst=blob_name,
            bucket=RAW_BUCKET,
            google_cloud_storage_conn_id="google_cloud_default",
        ).execute(context=context)


def load_to_bigquery(**context):
    for local_file in pathlib.Path("/tmp").glob("*.csv"):
        table_name = local_file.stem.replace('-', '_')
        target_table = f"{PROJECT_ID}:{BQ_DATASET_RAW}.{table_name}"
        operator = GCSToBigQueryOperator(
            task_id=f"gcs2bq_{table_name}",
            bucket=RAW_BUCKET,
            source_objects=[f"raw/{local_file.name}"],
            destination_project_dataset_table=target_table,
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            bigquery_conn_id='google_cloud_default',
            google_cloud_storage_conn_id='google_cloud_default',
        )
        operator.execute(context=context)


def run_dbt(**context):
    dbt_base = "/opt/airflow/dbt"
    subprocess.check_call(["dbt", "deps", "--profiles-dir", dbt_base], cwd=dbt_base)
    subprocess.check_call(["dbt", "seed", "--profiles-dir", dbt_base], cwd=dbt_base)
    subprocess.check_call(["dbt", "run", "--profiles-dir", dbt_base], cwd=dbt_base)
    subprocess.check_call(["dbt", "test", "--profiles-dir", dbt_base], cwd=dbt_base)


def check_terraform_state(**context):
    """Check if GCP resources already exist"""
    tf_dir = "/opt/airflow/terraform"
    try:
        # Check if terraform state file exists and has resources
        result = subprocess.run(
            ["terraform", "state", "list"],
            cwd=tf_dir,
            capture_output=True,
            text=True,
            timeout=30
        )
        return len(result.stdout.strip().split('\n')) > 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False


def provision_infrastructure(**context):
    """Provision GCP infrastructure using Terraform if not already exists"""
    tf_dir = "/opt/airflow/terraform"

    # Check if resources already exist
    if check_terraform_state():
        print("GCP resources already exist, skipping Terraform provisioning")
        return

    # Initialize and apply Terraform
    subprocess.check_call(["terraform", "init"], cwd=tf_dir)
    subprocess.check_call(["terraform", "apply", "-auto-approve"], cwd=tf_dir)

    # Get outputs for environment variables
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=tf_dir,
        capture_output=True,
        text=True
    )
    outputs = json.loads(result.stdout)

    # Set environment variables for downstream tasks
    context['ti'].xcom_push(key='bucket_name', value=outputs['bucket_name']['value'])
    context['ti'].xcom_push(key='bq_raw_dataset', value=outputs['bq_raw_dataset']['value'])
    context['ti'].xcom_push(key='bq_analytics_dataset', value=outputs['bq_analytics_dataset']['value'])


def run_spark(**context):
    spark_script = "/opt/airflow/spark/segmentation_reco.py"
    subprocess.check_call(["spark-submit", "--master", "spark://spark-master:7077", spark_script])


def refresh_looker_studio(**context):
    report_id = os.getenv("LOOKER_STUDIO_REPORT_ID")
    if not report_id:
        raise ValueError("LOOKER_STUDIO_REPORT_ID is not defined in the environment")

    # Use Google credentials from Airflow connection
    hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    creds = hook.get_credentials()
    if not creds.valid:
        creds.refresh(Request())

    token = creds.token
    if not token:
        raise RuntimeError("Could not retrieve an access token for Looker Studio API")

    url = f"https://datastudio.googleapis.com/v1/reports/{report_id}:refresh"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.post(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_default_args():
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

with DAG(
    dag_id='grocery_sales_end_to_end',
    default_args=get_default_args(),
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
) as dag:

    task_terraform = PythonOperator(task_id='provision_infrastructure', python_callable=provision_infrastructure)
    task_download = PythonOperator(task_id='download_kaggle', python_callable=download_kaggle)
    task_upload = PythonOperator(task_id='upload_to_gcs', python_callable=upload_to_gcs)
    task_ingest = PythonOperator(task_id='load_to_bigquery', python_callable=load_to_bigquery)
    task_dbt = PythonOperator(task_id='run_dbt', python_callable=run_dbt)
    task_spark = PythonOperator(task_id='run_spark', python_callable=run_spark)
    task_refresh_looker = PythonOperator(task_id='refresh_looker_studio', python_callable=refresh_looker_studio)

    task_terraform >> task_download >> task_upload >> task_ingest >> task_dbt >> task_spark >> task_refresh_looker
