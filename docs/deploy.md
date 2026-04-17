# Deployment

Production runs as two **Cloud Run Jobs** triggered by **Cloud
Scheduler**. Everything is codified under `deploy/terraform/`.

## Prerequisites

- GCP project `reitbrazil` with billing enabled.
- APIs enabled: BigQuery, Cloud Storage, Cloud Run, Cloud Scheduler,
  Artifact Registry, Secret Manager, IAM.
- `gcloud` CLI authenticated.
- `terraform` ≥ 1.7.
- brapi.dev Pro token.
- GitHub PAT with `contents:write` scope (for monthly release).

## One-time bootstrap

```bash
cd deploy/terraform
terraform init
terraform apply -var="project_id=reitbrazil"
```

After apply:

```bash
# Seed secrets (values not tracked in Terraform)
echo -n "$BRAPI_TOKEN" | gcloud secrets versions add brapi-token \
    --project=reitbrazil --data-file=-
echo -n "$GITHUB_TOKEN" | gcloud secrets versions add github-token \
    --project=reitbrazil --data-file=-
```

## Container

```bash
make docker-build
gcloud auth configure-docker southamerica-east1-docker.pkg.dev
make docker-push
```

The image is pinned in the Cloud Run Job spec via Terraform. When you
deploy a new version:

```bash
make docker-push DOCKER_TAG=v$(date +%Y%m%d)
terraform apply -var="project_id=reitbrazil" -var="image_tag=v$(date +%Y%m%d)"
```

## Manual run

```bash
gcloud run jobs execute reitbrazil-sync-daily --region=southamerica-east1
gcloud run jobs execute reitbrazil-sync-monthly --region=southamerica-east1
```

## Scheduled triggers

| Job | Cron | TZ | Purpose |
|---|---|---|---|
| `reitbrazil-sync-daily` | `15 22 * * 1-5` | America/Sao_Paulo | Daily prices/dividends |
| `reitbrazil-sync-monthly` | `30 23 2 * *` | America/Sao_Paulo | CVM + GitHub release |

## Costs (expected)

This pipeline fits comfortably in the GCP free tier:

- **BigQuery**: <1 GB storage, <100 MB scanned per run. Well under the
  1 TB/month query free tier.
- **Cloud Storage**: <1 GB total (latest + history + sidecars).
- **Cloud Run Jobs**: ~60 invocations/month × ~5 min CPU. Tier is
  limited but sufficient.
- **Artifact Registry**: <200 MB.

Expected monthly bill: **< US$ 1** at steady state.

## Observability

- Logs stream to Cloud Logging automatically (stderr from Cloud Run).
- A run report is written to `gs://reitbrazil-db/runs/<YYYY-MM-DD>.json`.
- **Next step (v1.1)**: Cloud Monitoring log-based alerts on `level=error`
  count > N in a rolling window.

## Disaster recovery

- Raw layer is the source of truth. If canon or exports get corrupted,
  rerun transforms and export; raw never changes.
- GCS `history/reitbrazil-YYYY-MM-DD.db` plus monthly GitHub releases
  give offsite backups with 1-year retention.
