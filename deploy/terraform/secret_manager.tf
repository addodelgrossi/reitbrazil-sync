resource "google_secret_manager_secret" "brapi_token" {
  project   = var.project_id
  secret_id = var.brapi_token_secret

  replication {
    auto {}
  }

  depends_on = [google_project_service.services]
}

resource "google_secret_manager_secret" "github_token" {
  project   = var.project_id
  secret_id = var.github_token_secret

  replication {
    auto {}
  }

  depends_on = [google_project_service.services]
}
