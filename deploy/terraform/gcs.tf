resource "google_storage_bucket" "db" {
  project                     = var.project_id
  name                        = var.gcs_bucket
  location                    = var.bq_location
  force_destroy               = false
  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  versioning {
    enabled = true
  }

  # Expire dated history copies after a year; the latest/ object remains.
  lifecycle_rule {
    condition {
      age            = 365
      matches_prefix = ["history/"]
      with_state     = "ANY"
    }
    action {
      type = "Delete"
    }
  }

  # Purge old noncurrent versions to keep costs bounded.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 90
      with_state                 = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.services]
}
