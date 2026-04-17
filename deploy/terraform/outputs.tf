output "daily_job" {
  value       = google_cloud_run_v2_job.daily.name
  description = "Name of the Cloud Run Job that runs the daily pipeline."
}

output "monthly_job" {
  value       = google_cloud_run_v2_job.monthly.name
  description = "Name of the Cloud Run Job that runs the monthly pipeline."
}

output "bucket" {
  value       = google_storage_bucket.db.name
  description = "GCS bucket hosting the SQLite artifact."
}

output "runner_service_account" {
  value       = google_service_account.runner.email
  description = "Service account used by the Cloud Run Jobs."
}

output "scheduler_service_account" {
  value       = google_service_account.scheduler.email
  description = "Service account used by Cloud Scheduler to invoke the jobs."
}

output "image" {
  value       = local.image
  description = "Full docker image path used by the Cloud Run Jobs."
}
