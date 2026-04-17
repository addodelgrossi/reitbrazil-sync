# Enable every API the pipeline depends on. Terraform re-enables are
# idempotent; destroying does not automatically disable APIs.

locals {
  required_services = [
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
  ]
}

resource "google_project_service" "services" {
  for_each = toset(local.required_services)

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}
