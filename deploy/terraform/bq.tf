resource "google_bigquery_dataset" "raw" {
  project                    = var.project_id
  dataset_id                 = "reitbrazil_raw"
  location                   = var.bq_location
  description                = "Bronze layer for reitbrazil-sync ingestion (append-only)."
  delete_contents_on_destroy = false

  depends_on = [google_project_service.services]
}

resource "google_bigquery_dataset" "canon" {
  project                    = var.project_id
  dataset_id                 = "reitbrazil_canon"
  location                   = var.bq_location
  description                = "Silver layer for reitbrazil-sync (deduplicated canonical tables)."
  delete_contents_on_destroy = false

  depends_on = [google_project_service.services]
}
