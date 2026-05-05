variable "project_id" {
    description = "GCP project ID"
    type        = string
    default     = "edwards-platform"
  }

  variable "region" {
    description = "GCP region"
    type        = string
    default     = "us-central1"
  }

  variable "bucket_name" {
    description = "GCS data lake bucket name"
    type        = string
    default     = "edwards-datalake"
  }

  variable "bq_dataset" {
    description = "BigQuery dataset ID"
    type        = string
    default     = "edwards_dw"
  }

  variable "composer_env_name" {
    description = "Cloud Composer environment name"
    type        = string
    default     = "edwards-composer"
  }