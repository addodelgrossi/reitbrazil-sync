#!/usr/bin/env bash
# dry-run.sh — shows which resources would be provisioned by Terraform,
# without needing live credentials. Useful for reviewing the deploy plan
# offline.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cat <<'SUMMARY'
reitbrazil-sync Terraform plan summary (static):

- BigQuery datasets:  reitbrazil_raw, reitbrazil_canon (southamerica-east1)
- GCS bucket:         reitbrazil-db (versioning on, 365d expiry on history/)
- Artifact Registry:  reitbrazilctl (DOCKER)
- Secret Manager:     brapi-token, github-token
- IAM:
    * reitbrazilctl-runner: BigQuery data editor + job user, GCS objectAdmin,
                             Secret accessor on brapi-token + github-token
    * reitbrazilctl-scheduler: run.invoker
- Cloud Run Jobs:     reitbrazil-sync-daily (run daily),
                       reitbrazil-sync-monthly (run monthly)
- Cloud Scheduler:    daily  22:15 BRT weekdays,
                       monthly 23:30 BRT day 2 of each month

Run `terraform -chdir=${DIR}/terraform plan -var="project_id=<your project>"`
to see the exact JSON plan.
SUMMARY
