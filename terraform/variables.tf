variable "credentials" {
  description = "My Credentials"
  default     = "/home/NEOPOSTADVPatel_na/.gc/ny-rides.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
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
  default   = "airflow"
}
