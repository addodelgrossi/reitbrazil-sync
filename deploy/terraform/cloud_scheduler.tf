locals {
  run_jobs_uri = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs"
}

resource "google_cloud_scheduler_job" "daily" {
  name        = "reitbrazil-sync-daily"
  description = "Trigger the daily ingestion job."
  schedule    = var.daily_cron
  time_zone   = var.schedule_timezone
  region      = var.region
  project     = var.project_id

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "${local.run_jobs_uri}/${google_cloud_run_v2_job.daily.name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_cloud_scheduler_job" "monthly" {
  name        = "reitbrazil-sync-monthly"
  description = "Trigger the monthly ingestion job (CVM + GitHub release)."
  schedule    = var.monthly_cron
  time_zone   = var.schedule_timezone
  region      = var.region
  project     = var.project_id

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "${local.run_jobs_uri}/${google_cloud_run_v2_job.monthly.name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [google_project_service.services]
}
