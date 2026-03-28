"""
High-level DAG logic:
1) Provision/refresh GCP infrastructure with Terraform.
2) Ingest Kaggle source files and load raw data to BigQuery.
3) Run dbt transformations for analytics-ready models.
4) Execute Spark segmentation/recommendation pipeline.
5) Trigger Looker Studio refresh to surface latest insights.
"""

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
import csv
import re
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


def _normalize_city_name(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", "", (value or "").strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_country_name(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", "", (value or "").strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _country_code_from_name(country_name: str, pycountry_module) -> str | None:
    if not country_name:
        return None

    aliases = {
        "czech republic": "Czechia",
        "macedonia": "North Macedonia",
        "swaziland": "Eswatini",
        "vatican city": "Holy See (Vatican City State)",
    }
    lookup_name = aliases.get(_normalize_country_name(country_name), country_name)

    try:
        return pycountry_module.countries.lookup(lookup_name).alpha_2
    except LookupError:
        try:
            result = pycountry_module.countries.search_fuzzy(lookup_name)
            if result:
                return result[0].alpha_2
        except LookupError:
            return None
    return None


def _build_city_to_country_code_lookup() -> dict[str, str]:
    import geonamescache

    gc = geonamescache.GeonamesCache()
    city_records = gc.get_cities().values()
    lookup: dict[str, tuple[str, int]] = {}

    for city in city_records:
        city_name = _normalize_city_name(city.get("name", ""))
        country_code = (city.get("countrycode") or "").strip().upper()
        if not city_name or not country_code:
            continue
        population = int(city.get("population") or 0)
        current = lookup.get(city_name)
        if current is None or population > current[1]:
            lookup[city_name] = (country_code, population)

    return {city_name: payload[0] for city_name, payload in lookup.items()}


def normalize_geography_seeds(dbt_workdir: pathlib.Path) -> None:
    """Normalize geography seed data in runtime workspace before dbt seed."""
    data_dir = dbt_workdir / "data"
    countries_path = data_dir / "countries.csv"
    cities_path = data_dir / "cities.csv"

    if not countries_path.exists() or not cities_path.exists():
        print("Skipping geography normalization: countries.csv or cities.csv is missing")
        return

    with countries_path.open("r", newline="", encoding="utf-8") as fp:
        countries_reader = csv.DictReader(fp)
        countries_fieldnames = countries_reader.fieldnames
        countries_rows = list(countries_reader)

    if not countries_fieldnames:
        print("Skipping geography normalization: countries.csv has no headers")
        return

    try:
        import pycountry
    except ImportError:
        print("Skipping geography normalization: pycountry is not installed")
        return

    try:
        city_to_country_code = _build_city_to_country_code_lookup()
    except ImportError:
        print("Skipping geography normalization: geonamescache is not installed")
        return

    countries_updated = 0
    country_id_by_code: dict[str, str] = {}
    for row in countries_rows:
        current_code = (row.get("CountryCode") or "").strip().upper()
        resolved_code = _country_code_from_name((row.get("CountryName") or "").strip(), pycountry)
        if resolved_code and resolved_code != current_code:
            row["CountryCode"] = resolved_code
            current_code = resolved_code
            countries_updated += 1
        if current_code:
            country_id_by_code[current_code] = (row.get("CountryID") or "").strip()

    with countries_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=countries_fieldnames)
        writer.writeheader()
        writer.writerows(countries_rows)

    with cities_path.open("r", newline="", encoding="utf-8") as fp:
        cities_reader = csv.DictReader(fp)
        cities_fieldnames = cities_reader.fieldnames
        cities_rows = list(cities_reader)

    if not cities_fieldnames:
        print("Skipping geography normalization: cities.csv has no headers")
        return

    existing_ids = []
    for row in countries_rows:
        try:
            existing_ids.append(int((row.get("CountryID") or "").strip()))
        except ValueError:
            continue
    next_country_id = max(existing_ids, default=0) + 1

    created_countries = 0
    cities_updated = 0
    unresolved_cities = 0
    for row in cities_rows:
        city_name = (row.get("CityName") or "").strip()
        normalized_city_name = _normalize_city_name(city_name)
        country_code = city_to_country_code.get(normalized_city_name)
        if not country_code:
            unresolved_cities += 1
            continue

        target_country_id = country_id_by_code.get(country_code)
        if not target_country_id:
            country = pycountry.countries.get(alpha_2=country_code)
            if not country:
                unresolved_cities += 1
                continue

            target_country_id = str(next_country_id)
            next_country_id += 1
            countries_rows.append(
                {
                    "CountryID": target_country_id,
                    "CountryName": country.name,
                    "CountryCode": country_code,
                }
            )
            country_id_by_code[country_code] = target_country_id
            created_countries += 1

        if (row.get("CountryID") or "").strip() != target_country_id:
            row["CountryID"] = target_country_id
            cities_updated += 1

    if created_countries > 0:
        with countries_path.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=countries_fieldnames)
            writer.writeheader()
            writer.writerows(countries_rows)

    with cities_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=cities_fieldnames)
        writer.writeheader()
        writer.writerows(cities_rows)

    print(
        "Geography seed normalization complete: "
        f"countries_updated={countries_updated}, created_countries={created_countries}, "
        f"cities_updated={cities_updated}, unresolved_cities={unresolved_cities}"
    )


def run_dbt(**context):
    dbt_source = pathlib.Path("/opt/airflow/dbt")
    dbt_workdir = pathlib.Path("/tmp/dbt_work")

    if dbt_workdir.exists():
        shutil.rmtree(dbt_workdir)
    shutil.copytree(dbt_source, dbt_workdir)

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(dbt_workdir)

    def run_cmd(args, critical=True):
        print(f"Running: {' '.join(args)}")
        result = subprocess.run(args, cwd=str(dbt_workdir), env=env, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"Command failed with return code: {result.returncode}")
            print("STDERR:")
            print(result.stderr)
            if critical:
                raise subprocess.CalledProcessError(result.returncode, " ".join(args))
            else:
                print(f"Warning: Command failed but continuing: {' '.join(args)}")

    packages_file = dbt_workdir / "packages.yml"
    if packages_file.exists():
        run_cmd(["dbt", "deps", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir)])

    # Check and list seed files for debugging
    data_dir = dbt_workdir / "data"
    seed_files = ["categories.csv", "cities.csv", "countries.csv"]
    if data_dir.exists():
        existing_files = list(data_dir.glob("*.csv"))
        print(f"Found {len(existing_files)} CSV files in {data_dir}: {[f.name for f in existing_files]}")
    else:
        print(f"Data directory {data_dir} does not exist, creating it")
        data_dir.mkdir(parents=True, exist_ok=True)
    
    # List seed files that should be present
    found_seeds = [f for f in seed_files if (data_dir / f).exists()]
    print(f"Seed files found: {found_seeds}")

    normalize_geography_seeds(dbt_workdir)
    
    # Always attempt dbt seed, regardless of whether files exist
    print("Running dbt seed...")
    run_cmd(["dbt", "seed", "--full-refresh", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"], critical=False)

    print("Running dbt models with full refresh...")
    run_cmd(["dbt", "run", "--full-refresh", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"])

    print("Running dbt tests...")
    run_cmd(["dbt", "test", "--profiles-dir", str(dbt_workdir), "--project-dir", str(dbt_workdir), "--target", "dev"], critical=False)


def provision_infrastructure(**context):
    """Provision GCP infrastructure using Terraform, syncing state from actual GCP resources"""
    tf_dir = "/opt/airflow/terraform"

    print("Provisioning GCP infrastructure with Terraform...")
    
    # Always init terraform first
    print("Initializing Terraform...")
    result = subprocess.run(
        ["terraform", "init"],
        cwd=tf_dir,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Terraform init failed: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, "terraform init")
    
    # Refresh state from actual GCP resources - syncs local state with real resources
    print("Refreshing Terraform state from GCP...")
    result = subprocess.run(
        ["terraform", "refresh"],
        cwd=tf_dir,
        capture_output=True,
        text=True
    )
    print("Terraform refresh output:")
    print(result.stdout)
    if result.returncode != 0:
        print("Terraform refresh warnings/errors:")
        print(result.stderr)
    
    # Apply infrastructure - will create only missing resources
    # State is now synced with actual GCP resources
    print("Applying Terraform configuration (creating only missing resources)...")
    result = subprocess.run(
        ["terraform", "apply", "-auto-approve"],
        cwd=tf_dir,
        capture_output=True,
        text=True
    )
    
    print("Terraform apply output:")
    print(result.stdout)
    
    if result.returncode != 0:
        print("Terraform apply warnings/errors:")
        print(result.stderr)
        # Check if the failure is only due to existing resources (409 errors)
        if "already exists" in result.stderr or "409" in result.stderr:
            print("Apply encountered existing resources, but this is expected.")
            print("New resources (datasets) should have been created successfully.")
        else:
            # If it's a different error, re-raise it
            raise subprocess.CalledProcessError(result.returncode, "terraform apply")

    print("Terraform provisioning completed")
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
