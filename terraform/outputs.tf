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
