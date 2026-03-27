# High-level logic:
# Declares Terraform input parameters for environment configuration,
# credentials, and runtime secrets used by the platform.

variable "credentials" {
  description = "GCP Service Account JSON credentials file path"
  default     = "/opt/airflow/gcp-key.json"
}

variable "project_id" {
  type = string
}

variable "location" {
  type    = string
  default = "US"
}

variable "region" {
  type    = string
}

variable "kaggle_api_token" {
  type      = string
  sensitive = true
}

variable "looker_studio_report_id" {
  type      = string
  sensitive = true
}

variable "airflow_db_password" {
  type      = string
  sensitive = true
}
