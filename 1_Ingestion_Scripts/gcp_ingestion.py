#%% Importing Libraries
import json
import os
from pathlib import Path
from datetime import timedelta

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# Importing existing code ingestion module 
import gcp_ingestion_function as ingestion_functions

# ---------- CONFIGURATION ----------
# Pull configuration from JSON
with open("client_config.json", "r") as file:
    data = json.load(file)

CLIENT_OPERATOR_NAME = data["client_operator_name"]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/moadmin/Desktop/Programming_Projects/Petrinex_Analysis/Scripts/Staging/emissionsdashboard-2aac4ff5ab2d.json'
BUCKET_NAME = data["gcp_configuration"]["bucket_name"]
PREFIX = data["gcp_configuration"]["bucket_prefix"]
PROJECT_ID = data["gcp_configuration"]["project_id"]

CLIENT_INFASTRUCTURE_FOLDER = data["local_configuration"]["client_infastructure_folder"]
CLIENT_VOLUMETRIC_FOLDER = data["local_configuration"]["client_volumetric_folder"]
DESTINATION_FOLDER = data["local_configuration"]["pre_stage"]

BIGQUERY_DATASET = data["gcp_configuration"]["bigquery_dataset"]

print(f"""
🚀 Configuration Loaded for: {CLIENT_OPERATOR_NAME}
{"-"*40}
GCP Settings:
  • Project ID:  {PROJECT_ID}
  • Bucket:      {BUCKET_NAME}
  • Prefix:      {PREFIX}
  • BQ Dataset:  {BIGQUERY_DATASET}

Local Paths:
  • Infrastructure: {CLIENT_INFASTRUCTURE_FOLDER}
  • Volumetric:     {CLIENT_VOLUMETRIC_FOLDER}
  • Staging Area:   {DESTINATION_FOLDER}
{"-"*40}
""")


# ------------------------ MAIN FLOW EXECUTION --------------------------- #

# ----------------------------- TASKS SETUP ------------------------------ #
# ------------------------------------------------------------------------ #
@task
def run_filter_infrastructure():
    logger = get_run_logger()
    logger.info("Filtering infastructure data")
    ingestion_functions.filter_infastructure_to_target(
        folder_path=CLIENT_INFASTRUCTURE_FOLDER,
        client_name=CLIENT_OPERATOR_NAME,
        target_folder=DESTINATION_FOLDER)
    return True


@task
def build_volumetrics_csv():
    logger = get_run_logger()
    logger.info("Appending volumetric data and saving to CSV")
    volumetrics_df = ingestion_functions.volumetrics_to_target(
        volumetrics_folder=CLIENT_VOLUMETRIC_FOLDER,
        client_name=CLIENT_OPERATOR_NAME
    )
    output_path = f"{DESTINATION_FOLDER}/{CLIENT_OPERATOR_NAME}_volumetrics.csv"
    volumetrics_df.write_csv(output_path)
    logger.info(f"Volumetric data saved to {output_path}")
    return output_path

@task
def upload_to_bigquery():
    logger = get_run_logger()
    logger.info("Uploading CSV files to BigQuery")
    ingestion_functions.stream_to_bq(
        source_folder_location=DESTINATION_FOLDER,
        project_id=PROJECT_ID,
        dataset_name=BIGQUERY_DATASET
    )
    logger.info("Upload to BigQuery completed")
    return True

@task
def upload_to_gcs():
    logger = get_run_logger()
    logger.info("Uploading CSV files to GCS")
    ingestion_functions.stream_to_gsc(
        source_folder_location=DESTINATION_FOLDER,
        bucket_name=BUCKET_NAME,
        prefix=PREFIX
    )
    logger.info("Upload to GCS completed")
    return True

# ----------------------------- FLOW SETUP ------------------------------- #
# ------------------------------------------------------------------------ #
@flow(name="Petrinex-ingestion-flow")
def ingestion_flow():
    logger = get_run_logger()
    logger.info(f"🔄 Starting ingestion flow for {CLIENT_OPERATOR_NAME}")

    # Step 1: Filter Infrastructure Data
    run_filter_infrastructure()

    # Step 2: Build Volumetrics CSV
    volumetrics_csv_path = build_volumetrics_csv()

    # Step 3: Upload to GCS
    upload_to_gcs()

    # Step 4: Upload to BigQuery
    upload_to_bigquery()

    logger.info(f"✅ Ingestion flow for {CLIENT_OPERATOR_NAME} completed successfully.")


if __name__ == "__main__":
    ingestion_flow()
