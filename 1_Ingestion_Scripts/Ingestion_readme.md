# Petrinex — GCP Ingestion

Short README documenting the ingestion process, how to run locally and how to deploy with Prefect.

## Overview
This repo contains ingestion code that:
- Prepares client infrastructure and volumetric CSVs
- Uploads staged CSVs to Google Cloud Storage and BigQuery
- Is orchestrated via Prefect (flow: `ingestion_flow` in `gcp_ingestion.py`)

Core files
- `gcp_ingestion.py` — Prefect flow + tasks wrapper
- `gcp_ingestion_function.py` — core ingestion functions (filtering, building CSVs, uploads)
- `client_config.json` — environment/config for client and GCP
- `prefect_ingestion_config.yaml` — deployment config for Prefect
- Credentials file (not checked in): JSON service account for GCP

---

## Repo layout (relevant)
- Scripts/Staging/
  - gcp_ingestion.py
  - gcp_ingestion_function.py
  - client_config.json
  - prefect_ingestion_config.yaml
  - emissionsdashboard-<redacted>.json (local credential file — DO NOT commit)

---

## Prerequisites (macOS)
- Python 3.9+
- Prefect CLI (your environment uses Prefect v3.6.11)
- Google Cloud SDK libs if used by ingestion functions:
  - `pip install "prefect>=3.6.11" google-cloud-storage google-cloud-bigquery`

---

## Configuration
1. Edit `client_config.json` to reflect client and GCP settings. Example structure:
```json
{
  "client_operator_name": "ENHANCE ENERGY INC.",
  "gcp_configuration": {
    "bucket_name": "petrinex_source_data",
    "bucket_prefix": "alberta",
    "project_id": "emissionsdashboard",
    "bigquery_dataset": "staging"
  },
  "local_configuration": {
    "client_infastructure_folder": "/path/to/Petrinex Source Data",
    "client_volumetric_folder": "/path/to/Petrinex Source Data/Volumetrics",
    "pre_stage": "/path/to/Petrinex Source Data/Pre_Ingestion_Source_Data"
  }
}
```

2. Provide GCP credentials to the runtime environment. Preferred approaches:
- Local testing:
  - export GOOGLE_APPLICATION_CREDENTIALS="/full/path/to/emissionsdashboard-<id>.json"
- Containerized deployments:
  - Mount credentials into the container and set `GOOGLE_APPLICATION_CREDENTIALS` to that path (see `prefect_ingestion_config.yaml`).

Never commit the credentials file.

---

## Running locally (quick test)
From the staging directory:
```bash
cd /Users/moadmin/Desktop/Programming_Projects/Petrinex_Analysis/Scripts/Staging
export GOOGLE_APPLICATION_CREDENTIALS="/Users/moadmin/.../emissionsdashboard-<id>.json"
python gcp_ingestion.py
```
This imports the flow and executes the `ingestion_flow()` when run as a script. Use logs printed to the console for debugging.

---

## Prefect deployment & scheduling
1. Update `prefect_ingestion_config.yaml` schedule/timezone to your desired schedule (example in repo uses `America/Denver`).
2. Apply the deployment:
```bash
cd /Users/moadmin/Desktop/Programming_Projects/Petrinex_Analysis/Scripts/Staging
prefect deployment apply ./prefect_ingestion_config.yaml
prefect deployment ls
```
3. Ensure an execution worker/agent is running and bound to the configured work queue:
```bash
# start a local agent (or use your container/k8s worker)
prefect agent start --work-queue default &> ~/prefect_agent.log &
tail -f ~/prefect_agent.log
```
4. Verify in the Prefect UI:
- Open the UI and check Deployment → automated-ingestion → Next scheduled run
- If you want to trigger immediately for testing:
```bash
prefect deployment run "petrinex-gcp-ingestion/automated-ingestion"
```

---

## Common troubleshooting
- Deployment shows queued but never runs: no agent picked up the run. Start/inspect the agent logs.
- Prefect server/CLI issues:
  - Check Prefect version: `prefect --version`
  - If UI not reachable, check listeners: `lsof -iTCP:4200 -sTCP:LISTEN` (or Prefect v3 UI port used)
- Import-time errors (Prefect fails to start): run `python -c "import gcp_ingestion"` to surface errors.
- CSV write errors: ensure `pre_stage` folder exists (task should create it automatically).
- DataFrame method mismatch: confirm dataframe object uses `to_csv` (pandas) or `write_csv` (polars).

Useful commands:
```bash
# view deployments
prefect deployment ls

# trigger a deployment run
prefect deployment run "<PROJECT>/<DEPLOYMENT_NAME>"

# tail agent logs
tail -f ~/prefect_agent.log
```

---

## Best practices
- Keep credentials out of VCS — use host env vars or Prefect Secrets / mounted credentials.
- Load config at runtime (inside flow or a load_config function) to avoid import-time failures when Prefect imports modules.
- Ensure tasks with side effects are not cached (remove `cache_key_fn` / `cache_expiration`) unless safe.
- Use descriptive logging and retries on network/GCP operations.

---

## Contact / next steps
- For a one-off run: trigger the deployment via the UI or `prefect deployment run`.
- For changes to scheduling or environment, update `prefect_ingestion_config.yaml`, `client_config.json`, and re-
