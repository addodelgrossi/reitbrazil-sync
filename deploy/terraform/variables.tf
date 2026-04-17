variable "project_id" {
  type        = string
  description = "GCP project id that hosts the ingestion pipeline."
}

variable "region" {
  type        = string
  default     = "southamerica-east1"
  description = "Default region for Cloud Run Jobs, Artifact Registry, and BigQuery."
}

variable "bq_location" {
  type        = string
  default     = "southamerica-east1"
  description = "BigQuery dataset location."
}

variable "gcs_bucket" {
  type        = string
  default     = "reitbrazil-db"
  description = "GCS bucket for the published SQLite artifact."
}

variable "artifact_registry_repo" {
  type        = string
  default     = "reitbrazilctl"
  description = "Artifact Registry repo for reitbrazilctl container images."
}

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag to deploy to the Cloud Run Jobs."
}

variable "brapi_token_secret" {
  type        = string
  default     = "brapi-token"
  description = "Secret Manager secret holding the brapi.dev API token."
}

variable "github_token_secret" {
  type        = string
  default     = "github-token"
  description = "Secret Manager secret holding the GitHub PAT for monthly releases."
}

variable "daily_cron" {
  type        = string
  default     = "15 22 * * 1-5"
  description = "Cloud Scheduler cron for the daily pipeline (America/Sao_Paulo)."
}

variable "monthly_cron" {
  type        = string
  default     = "30 23 2 * *"
  description = "Cloud Scheduler cron for the monthly pipeline (America/Sao_Paulo)."
}

variable "schedule_timezone" {
  type        = string
  default     = "America/Sao_Paulo"
  description = "Timezone for Cloud Scheduler crons."
}
