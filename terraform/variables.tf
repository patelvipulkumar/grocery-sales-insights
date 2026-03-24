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
