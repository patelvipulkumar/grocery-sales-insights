# High-level logic:
# Exposes provisioned resource identifiers so orchestration and deployment
# steps can consume infrastructure metadata reliably.

output "service_account_email" {
  value = google_service_account.airflow.email
}

output "bucket_name" {
  value = google_storage_bucket.raw_bucket.name
}

output "bq_raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}

output "kaggle_api_secret_name" {
  value = google_secret_manager_secret.kaggle_api.id
}

output "looker_studio_secret_name" {
  value = google_secret_manager_secret.looker_studio_report_id.id
}

output "airflow_db_password_secret_name" {
  value = google_secret_manager_secret.airflow_db_password.id
}
