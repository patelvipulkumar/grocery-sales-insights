# High-level logic:
# Provisions core GCP infrastructure for the data platform, including storage,
# BigQuery datasets, service account permissions, and required secrets.

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region

}

resource "google_service_account" "airflow" {
  account_id   = "airflow-sa"
  display_name = "Airflow service account"
}

resource "google_storage_bucket" "raw_bucket" {
  name                        = "grocery-raw"
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "grocery_raw"
  location   = var.location
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "grocery_analytics"
  location   = var.location
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "grocery_staging"
  location   = var.location
}

resource "google_secret_manager_secret" "kaggle_api" {
  secret_id = "kaggle-api-token"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "kaggle_api_version" {
  secret      = google_secret_manager_secret.kaggle_api.id
  secret_data = var.kaggle_api_token
}

resource "google_secret_manager_secret" "looker_studio_report_id" {
  secret_id = "looker-studio-report-id"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "looker_studio_report_id_version" {
  secret      = google_secret_manager_secret.looker_studio_report_id.id
  secret_data = var.looker_studio_report_id
}

resource "google_secret_manager_secret" "airflow_db_password" {
  secret_id = "airflow-db-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "airflow_db_password_version" {
  secret      = google_secret_manager_secret.airflow_db_password.id
  secret_data = var.airflow_db_password
}

resource "google_storage_bucket_iam_member" "airflow_raw_bucket" {
  bucket = google_storage_bucket.raw_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_analytics" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_staging" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_secret_manager_secret_iam_member" "airflow_kaggle" {
  secret_id = google_secret_manager_secret.kaggle_api.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_secret_manager_secret_iam_member" "airflow_looker" {
  secret_id = google_secret_manager_secret.looker_studio_report_id.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_secret_manager_secret_iam_member" "airflow_db_password" {
  secret_id = google_secret_manager_secret.airflow_db_password.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow.email}"
}
