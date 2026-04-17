resource "google_artifact_registry_repository" "reitbrazilctl" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repo
  description   = "Container images for reitbrazilctl."
  format        = "DOCKER"

  depends_on = [google_project_service.services]
}
