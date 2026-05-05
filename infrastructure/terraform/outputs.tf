 output "bucket_name" {
    description = "GCS data lake bucket name"
    value       = google_storage_bucket.datalake.name
  }

  output "bq_dataset" {
    description = "BigQuery dataset ID"
    value       = google_bigquery_dataset.edwards_dw.dataset_id
  }

  output "composer_environment" {
    description = "Cloud Composer environment name"
    value       = google_composer_environment.edwards_composer.name
  }

  output "pubsub_topic" {
    description = "Pub/Sub topic name"
    value       = google_pubsub_topic.device_events.name
  }