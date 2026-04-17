# Terraform — reitbrazil-sync

Provisions everything needed to run `reitbrazilctl` on Cloud Run Jobs
with Cloud Scheduler triggers.

## Resources

- `reitbrazil_raw`, `reitbrazil_canon` — BigQuery datasets
  (`southamerica-east1`).
- `gs://reitbrazil-db` — versioned GCS bucket with lifecycle cleanup.
- Artifact Registry repo `reitbrazilctl` for container images.
- Secret Manager secrets `brapi-token` and `github-token`.
- Service accounts:
  - `reitbrazilctl-runner` (least-privilege: BigQuery edit/job, GCS
    object admin scoped to `reitbrazil-db`, Secret Accessor on the two
    secrets).
  - `reitbrazilctl-scheduler` (Cloud Run invoker only).
- Cloud Run Jobs `reitbrazil-sync-daily` and `reitbrazil-sync-monthly`.
- Cloud Scheduler triggers for both jobs.

## First-time apply

```bash
cd deploy/terraform
terraform init
terraform apply -var="project_id=reitbrazil"
```

After apply, seed the secrets (values are not tracked in Terraform):

```bash
echo -n "$BRAPI_TOKEN" | gcloud secrets versions add brapi-token \
    --project=reitbrazil --data-file=-
echo -n "$GITHUB_TOKEN" | gcloud secrets versions add github-token \
    --project=reitbrazil --data-file=-
```

Build and push the image for the first time (outside Terraform):

```bash
make docker-build docker-push DOCKER_TAG=$(git rev-parse --short HEAD)
terraform apply -var="project_id=reitbrazil" \
    -var="image_tag=$(git rev-parse --short HEAD)"
```

## Verify

```bash
gcloud run jobs execute reitbrazil-sync-daily --region=southamerica-east1 --project=reitbrazil
gsutil ls gs://reitbrazil-db/latest/
```

## Cost expectations

Well within the GCP free tier (BigQuery 1 TB query/month, GCS 5 GB,
Cloud Run Jobs small-scale). Expected monthly bill: **< US$ 1** at
steady state.

## Re-running

`terraform apply` is idempotent. You can safely bump `image_tag`
variables and re-run; the Cloud Run Job will pick up the new revision.

## Destroy

```bash
terraform destroy -var="project_id=reitbrazil"
```

Note: BigQuery datasets and the GCS bucket have `delete_contents_on_destroy = false`
(resp. `force_destroy = false`) to protect production data. Empty them
manually before destroy if needed.
