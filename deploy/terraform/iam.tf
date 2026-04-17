resource "google_service_account" "runner" {
  project      = var.project_id
  account_id   = "reitbrazilctl-runner"
  display_name = "reitbrazilctl Cloud Run Job runner"
  description  = "Runs reitbrazil-sync daily and monthly pipelines."
}

# BigQuery: edit raw data and run query jobs.
resource "google_bigquery_dataset_iam_member" "runner_raw_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_bigquery_dataset_iam_member" "runner_canon_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.canon.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_project_iam_member" "runner_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.runner.email}"
}

# GCS: write to the published bucket.
resource "google_storage_bucket_iam_member" "runner_objects" {
  bucket = google_storage_bucket.db.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runner.email}"
}

# Secret Manager: read-only access to the token secrets.
resource "google_secret_manager_secret_iam_member" "runner_brapi" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.brapi_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_secret_manager_secret_iam_member" "runner_github" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.github_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runner.email}"
}

# Cloud Scheduler needs to invoke Cloud Run Jobs on the runner's behalf.
resource "google_service_account" "scheduler" {
  project      = var.project_id
  account_id   = "reitbrazilctl-scheduler"
  display_name = "reitbrazilctl Cloud Scheduler invoker"
}

resource "google_project_iam_member" "scheduler_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}
