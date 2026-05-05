 # GCS Data Lake Bucket
  resource "google_storage_bucket" "datalake" {
    name          = var.bucket_name
    location      = "US-EAST1"
    force_destroy = false

    uniform_bucket_level_access = true

    lifecycle_rule {
      condition {
        age = 90
      }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }

  # BigQuery Dataset
  resource "google_bigquery_dataset" "edwards_dw" {
    dataset_id  = var.bq_dataset
    location    = "US"
    description = "Edwards Lifesciences data warehouse"
  }

  # Pub/Sub Topic
  resource "google_pubsub_topic" "device_events" {
    name = "device-events-raw"
  }

  # Composer Service Account
  resource "google_service_account" "composer_sa" {
    account_id   = "composer-sa"
    display_name = "Composer Service Account"
  }

  resource "google_project_iam_member" "composer_worker" {
    project = var.project_id
    role    = "roles/composer.worker"
    member  = "serviceAccount:${google_service_account.composer_sa.email}"
  }

  resource "google_project_iam_member" "composer_storage_admin" {
    project = var.project_id
    role    = "roles/storage.admin"
    member  = "serviceAccount:${google_service_account.composer_sa.email}"
  }

  resource "google_project_iam_member" "composer_bq_admin" {
    project = var.project_id
    role    = "roles/bigquery.admin"
    member  = "serviceAccount:${google_service_account.composer_sa.email}"
  }

  resource "google_project_iam_member" "composer_pubsub_admin" {
    project = var.project_id
    role    = "roles/pubsub.admin"
    member  = "serviceAccount:${google_service_account.composer_sa.email}"
  }

  # Cloud Composer Environment
  resource "google_composer_environment" "edwards_composer" {
    name   = var.composer_env_name
    region = var.region

    config {
      node_config {
        service_account = google_service_account.composer_sa.email
      }

      software_config {
        image_version = "composer-3-airflow-2.10.5-build.34"
        env_variables = {
          EDWARDS_BUCKET  = var.bucket_name
          GCP_PROJECT_ID  = var.project_id
        }
      }
    }
  }