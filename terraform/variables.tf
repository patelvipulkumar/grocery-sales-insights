variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "US"
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
  default   = "airflow"
}
