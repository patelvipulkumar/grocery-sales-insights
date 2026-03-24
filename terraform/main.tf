provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_service_account" "airflow" {
  account_id   = "airflow-sa"
  display_name = "Airflow service account"
}

resource "google_storage_bucket" "raw_bucket" {
  name                        = "${var.project_id}-grocery-raw"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "grocery_raw"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "grocery_analytics"
  location   = var.region
}

resource "google_secret_manager_secret" "kaggle_api" {
  secret_id = "kaggle-api-token"
  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "kaggle_api_version" {
  secret      = google_secret_manager_secret.kaggle_api.id
  secret_data = var.kaggle_api_token
}

resource "google_project_iam_binding" "bigquery" {
  role    = "roles/bigquery.dataEditor"
  members = ["serviceAccount:${google_service_account.airflow.email}"]
}

resource "google_project_iam_binding" "storage" {
  role    = "roles/storage.admin"
  members = ["serviceAccount:${google_service_account.airflow.email}"]
}
