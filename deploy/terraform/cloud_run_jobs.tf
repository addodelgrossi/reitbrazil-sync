locals {
  image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repo}/reitbrazilctl:${var.image_tag}"

  runner_env = [
    { name = "INGEST_GCP_PROJECT", value = var.project_id },
    { name = "INGEST_BQ_LOCATION", value = var.bq_location },
    { name = "INGEST_BQ_DATASET_RAW", value = google_bigquery_dataset.raw.dataset_id },
    { name = "INGEST_BQ_DATASET_CANON", value = google_bigquery_dataset.canon.dataset_id },
    { name = "INGEST_GCS_BUCKET", value = google_storage_bucket.db.name },
    { name = "INGEST_GCS_KEY_LATEST", value = "latest/reitbrazil.db" },
    { name = "INGEST_GCS_KEY_METADATA", value = "latest/metadata.json" },
    { name = "INGEST_GCS_PREFIX_HISTORY", value = "history" },
    { name = "INGEST_LOG_LEVEL", value = "info" },
    { name = "INGEST_LOG_FORMAT", value = "json" },
    { name = "INGEST_GITHUB_REPO", value = "addodelgrossi/reitbrazil" },
  ]
}

resource "google_cloud_run_v2_job" "daily" {
  name     = "reitbrazil-sync-daily"
  location = var.region
  project  = var.project_id

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.runner.email
      timeout         = "1800s"
      max_retries     = 1

      containers {
        image = local.image
        args  = ["run", "daily"]

        dynamic "env" {
          for_each = local.runner_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        env {
          name = "INGEST_BRAPI_TOKEN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.brapi_token.secret_id
              version = "latest"
            }
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_cloud_run_v2_job" "monthly" {
  name     = "reitbrazil-sync-monthly"
  location = var.region
  project  = var.project_id

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.runner.email
      timeout         = "3600s"
      max_retries     = 1

      containers {
        image = local.image
        args  = ["run", "monthly"]

        dynamic "env" {
          for_each = local.runner_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        env {
          name = "INGEST_BRAPI_TOKEN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.brapi_token.secret_id
              version = "latest"
            }
          }
        }

        env {
          name = "INGEST_GITHUB_TOKEN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.github_token.secret_id
              version = "latest"
            }
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.services]
}
