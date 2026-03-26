from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from datetime import datetime, timedelta
import os
import subprocess
import json
import pathlib
import shutil
import requests
from google.auth.transport.requests import Request

PROJECT_ID = os.getenv("GCP_PROJECT", "your-project-id")
RAW_BUCKET = os.getenv("RAW_BUCKET", "your-project-id-grocery-raw")
BQ_DATASET_RAW = "grocery_raw"
BQ_DATASET_ANALYTICS = "grocery_analytics"
KAGGLE_DATASET = "andrexibiza/grocery-sales-dataset"


def resolve_bucket_name(context) -> str:
    task_instance = context.get("ti")
    if task_instance:
        xcom_bucket = task_instance.xcom_pull(task_ids="provision_infrastructure", key="bucket_name")
        if xcom_bucket:
            return xcom_bucket

    env_bucket = os.getenv("RAW_BUCKET", "").strip()
    if env_bucket and env_bucket != "your-project-id-grocery-raw":
        return env_bucket

    return "grocery-raw"


def push_terraform_outputs_to_xcom(context, tf_dir: str) -> None:
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=tf_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return

    outputs = json.loads(result.stdout)
    context["ti"].xcom_push(key="bucket_name", value=outputs.get("bucket_name", {}).get("value", "grocery-raw"))
    context["ti"].xcom_push(key="bq_raw_dataset", value=outputs.get("bq_raw_dataset", {}).get("value", BQ_DATASET_RAW))
    context["ti"].xcom_push(key="bq_analytics_dataset", value=outputs.get("bq_analytics_dataset", {}).get("value", BQ_DATASET_ANALYTICS))


def fetch_kaggle_credentials() -> tuple[str, str]:
    env_username = os.getenv("KAGGLE_USERNAME", "").strip()
    env_key = os.getenv("KAGGLE_KEY", "").strip() or os.getenv("KAGGLE_API_TOKEN", "").strip()
    if env_username and env_key:
        return env_username, env_key

    try:
        sm = SecretsManagerHook(gcp_conn_id="google_cloud_default")
        secret = sm.get_secret("kaggle-api-token")
    except Exception:
        secret = None

    username = env_username
    key = env_key

    if isinstance(secret, dict):
        payload = secret.get("payload", {}).get("data", "")
        secret = payload or secret

    if isinstance(secret, str) and secret.strip():
        raw = secret.strip()
        try:
            parsed = json.loads(raw)
            username = parsed.get("username", username)
            key = parsed.get("key", key)
        except json.JSONDecodeError:
            key = raw

    if not username or not key:
        raise ValueError("Kaggle credentials missing. Set KAGGLE_USERNAME/KAGGLE_KEY or secret 'kaggle-api-token' as JSON {\"username\":\"...\",\"key\":\"...\"}.")

    return username, key


def download_kaggle(**context):
    username, token = fetch_kaggle_credentials()
    os.makedirs("/tmp/kaggle", exist_ok=True)
    with open("/tmp/kaggle/kaggle.json", "w") as f:
        f.write(json.dumps({"username": username, "key": token}))

    env = os.environ.copy()
    env["KAGGLE_CONFIG_DIR"] = "/tmp/kaggle"

    subprocess.check_call(["kaggle", "datasets", "download", "-d", KAGGLE_DATASET, "-p", "/tmp", "--unzip"], env=env)
    
    # Copy seed CSVs to dbt data directory
    dbt_data_dir = "/opt/airflow/dbt/data"
    os.makedirs(dbt_data_dir, exist_ok=True)
    seed_files = ["categories.csv", "cities.csv", "countries.csv"]
    for seed_file in seed_files:
        src = pathlib.Path("/tmp") / seed_file
        if src.exists():
            shutil.copy2(src, pathlib.Path(dbt_data_dir) / seed_file)
            print(f"Copied {seed_file} to {dbt_data_dir}")


def upload_to_gcs(**context):
    bucket_name = resolve_bucket_name(context)
    print(f"Uploading to bucket: {bucket_name}")
    
    # Seed files are managed by dbt, do not upload them to GCS
    seed_files = {"categories.csv", "cities.csv", "countries.csv"}
    
    for local_file in pathlib.Path("/tmp").glob("*.csv"):
        if local_file.name in seed_files:
            print(f"Skipping seed file: {local_file.name} (managed by dbt)")
            continue
            
        blob_name = f"raw/{local_file.name}"
        LocalFilesystemToGCSOperator(
            task_id=f"upload_{local_file.stem}",
            src=str(local_file),
            dst=blob_name,
            bucket=bucket_name,
            gcp_conn_id="google_cloud_default",
        ).execute(context=context)


def load_to_bigquery(**context):
    bucket_name = resolve_bucket_name(context)
    print(f"Loading from bucket: {bucket_name}")

    operator_context = dict(context)
    if "logical_date" not in operator_context or operator_context.get("logical_date") is None:
        dag_run = operator_context.get("dag_run")
        if dag_run is not None and getattr(dag_run, "logical_date", None) is not None:
            operator_context["logical_date"] = dag_run.logical_date
    if "logical_date" not in operator_context or operator_context.get("logical_date") is None:
        task_instance = operator_context.get("ti") or operator_context.get("task_instance")
        if task_instance is not None and getattr(task_instance, "logical_date", None) is not None:
            operator_context["logical_date"] = task_instance.logical_date
    if "logical_date" not in operator_context or operator_context.get("logical_date") is None:
        operator_context["logical_date"] = datetime.utcnow()
    
    # Seed files are managed by dbt, do not load them directly to BigQuery
    seed_files = {"categories.csv", "cities.csv", "countries.csv"}
    
    for local_file in pathlib.Path("/tmp").glob("*.csv"):
        if local_file.name in seed_files:
            print(f"Skipping seed file: {local_file.name} (managed by dbt)")
            continue
            
        table_name = local_file.stem.replace('-', '_')
        target_table = f"{PROJECT_ID}:{BQ_DATASET_RAW}.{table_name}"
        operator = GCSToBigQueryOperator(
            task_id=f"gcs2bq_{table_name}",
            bucket=bucket_name,
            source_objects=[f"raw/{local_file.name}"],
            destination_project_dataset_table=target_table,
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            gcp_conn_id='google_cloud_default',
        )
        operator.execute(context=operator_context)


def run_dbt(**context):
    dbt_source = pathlib.Path("/opt/airflow/dbt")
    dbt_workdir = pathlib.Path("/tmp/dbt_work")

    if dbt_workdir.exists():
        shutil.rmtree(dbt_workdir)
    shutil.copytree(dbt_source, dbt_workdir)

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(dbt_workdir)

    def run_cmd(args):
        print(f"Running: {' '.join(args)}")
        subprocess.check_call(args, cwd=str(dbt_workdir), env=env)

    packages_file = dbt_workdir / "packages.yml"
    if packages_file.exists():
        run_cmd(["dbt", "deps", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir)])

    seed_files = ["categories.csv", "cities.csv", "countries.csv"]
    has_seed = any((dbt_workdir / "data" / file_name).exists() for file_name in seed_files)
    if has_seed:
        run_cmd(["dbt", "seed", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"])

    run_cmd(["dbt", "run", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"])
    run_cmd(["dbt", "test", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"])


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
        resources = [line for line in result.stdout.splitlines() if line.strip()]
        return len(resources) > 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False


def provision_infrastructure(**context):
    """Provision GCP infrastructure using Terraform if not already exists"""
    tf_dir = "/opt/airflow/terraform"

    # Check if resources already exist
    if check_terraform_state():
        print("GCP resources already exist, skipping Terraform provisioning")
        push_terraform_outputs_to_xcom(context, tf_dir)
        return

    # Initialize and apply Terraform
    subprocess.check_call(["terraform", "init"], cwd=tf_dir)
    subprocess.check_call(["terraform", "apply", "-auto-approve"], cwd=tf_dir)

    push_terraform_outputs_to_xcom(context, tf_dir)


def run_spark(**context):
    spark_script = "/opt/airflow/spark/segmentation_reco.py"
    base_args = [
        "spark-submit",
        "--packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
        spark_script,
    ]

    cluster_cmd = ["spark-submit", "--master", "spark://spark-master:7077"] + base_args[1:]

    try:
        print("Running Spark job on standalone cluster")
        subprocess.check_call(cluster_cmd)
    except subprocess.CalledProcessError:
        print("Cluster Spark execution failed; retrying in local mode")
        local_cmd = ["spark-submit", "--master", "local[2]"] + base_args[1:]
        subprocess.check_call(local_cmd)


def refresh_looker_studio(**context):
    hook = SecretsManagerHook(gcp_conn_id="google_cloud_default")
    report_id = hook.get_secret(secret_id="looker-studio-report-id")
    
    if not report_id:
        raise ValueError("looker-studio-report-id is not defined in Secret Manager")

    # Use Google credentials from Airflow connection
    gcp_hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    creds = gcp_hook.get_credentials()
    if not creds.valid:
        creds.refresh(Request())

    token = creds.token
    if not token:
        raise RuntimeError("Could not retrieve an access token for Looker Studio API")

    url = f"https://lookerstudio.google.com/reporting/{report_id}/refresh"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.post(url, headers=headers, timeout=30)

    response_text = (r.text or "").strip()

    if r.status_code in (404, 405):
        print(
            "Looker Studio refresh endpoint is not available for this report/API setup "
            f"(status={r.status_code}). Skipping refresh task."
        )
        return {"status": "skipped", "http_status": r.status_code}

    if r.status_code == 500 and '"code":13' in response_text.replace(" ", ""):
        print(
            "Looker Studio returned a non-actionable server error for refresh "
            "(status=500, code=13). Skipping refresh task."
        )
        return {"status": "skipped", "http_status": r.status_code, "error_code": 13}

    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {"status": "success", "http_status": r.status_code}


def get_default_args():
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

with DAG(
    dag_id='grocery_sales_end_to_end',
    default_args=get_default_args(),
    schedule='@daily',
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
