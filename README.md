# Grocery Sales Insights

A complete data engineering pipeline for `drexibiza/grocery-sales-dataset`.

Components:
- Airflow orchestrates ETL using Python tasks and operator hooks
- GCS for raw data staging
- BigQuery for raw + transformed + model outputs
- DBT for modeling/transformation
- Spark for segmentation and recommendation pipeline
- Terraform to provision GCP infrastructure
- Secret Manager to store API keys and credentials

## Quick start
1. Copy `gcp-key.json` into repo root (or set `GOOGLE_APPLICATION_CREDENTIALS` to your key file path).
2. `gcloud auth application-default login`
3. `docker compose up --build`
4. Open Airflow at `http://localhost:8080` and trigger `grocery_sales_end_to_end`

## Terraform
```bash
terraform init
tensorraform apply -var="project_id=YOUR_PROJECT" -var="kaggle_api_token=YOUR_TOKEN"
```

## Airflow DAG
- `airflow/dags/grocery_pipeline.py`:
  1. download Kaggle dataset `drexibiza/grocery-sales-dataset`
  2. upload raw CSV to GCS under `raw/`
  3. load raw data to BigQuery dataset `grocery_raw`
  4. run DBT `dbt run` and `dbt test` (from `grocery_analytics` dataset)
  5. run Spark segmentation/recommendation

## DBT models
- `dbt/models/staging/st_sales.sql`
- `dbt/models/marts/customer_lifetime_value.sql`

## Spark job
- `spark/segmentation_reco.py` loads data from BigQuery and writes results back to BigQuery.

## Architecture diagram

```mermaid
flowchart LR
    A[Kaggle Dataset: drexibiza/grocery-sales-dataset] -->|download| B[Airflow DAG]
    B --> C[Google Cloud Storage (raw bucket)]
    C --> D[BigQuery (grocery_raw)]
    D --> E[DBT (staging -> marts) in BigQuery (grocery_analytics)]
    D --> F[Spark job (segmentation + recommendations)]
    F --> G[BigQuery (customer_segments + customer_recommendations)]

    B --> H[Secret Manager (kaggle-api-token, GCP creds)]
    B --> I[Terraform infra provisioning]

    subgraph Local Dev
      J[docker-compose: airflow, postgres, redis, spark]
      J -- runs --> B
      J -- contains --> E
      J -- contains --> F
    end

    subgraph GCP
      C
      D
      E
      F
      G
      H
      I
    end
```

