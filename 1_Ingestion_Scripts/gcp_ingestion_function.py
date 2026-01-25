import polars as pl
import re
import os
import json
from datetime import date

## Libraries for GCP
from google.cloud import storage 
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound
import mimetypes

# ---------------------------------------------------------------------------------------------------------- #
# ----------------------------------------------- FUNCTIONS ------------------------------------------------------- #
# ---------------------------------------------------------------------------------------------------------- #

# Utils Functions: 
def update_table_name(file_name):
    return re.sub(r"[^a-zA-Z0-9]", "", file_name)

# ------- Function to process Facility Infastructure Data
def filter_infastructure_to_target (folder_path, client_name, target_folder): 
    for file in os.listdir(folder_path):
        if file.lower().endswith(".csv") and "Infrastructure" in file:
            file_path = os.path.join(folder_path,file)
            print(f"🔄 Processing {file_path} for client: {client_name}")

            try:
                # Load data filter on client data
                df = pl.read_csv(file_path, ignore_errors=True)
                    
                
                print(f"✅ {file} read to polars DataFrame sucessfully")
                
                # Filter first to keep memory usage low
                filtered_df = df.filter(pl.col("OperatorName")==client_name).clone()
                print(f"✅ DateFrame Filtered to {client_name}")

                if not filtered_df.height==0:

                    # Add Metadata
                    filtered_df = filtered_df.with_columns([
                        pl.lit(date.today()).alias("effective_from_date"),
                        pl.lit(None).alias("effective_to_date"),
                        pl.lit("True").alias("is_current")   
                    ])
                    print("✅ MetaDate Column Added")


                    # Save the filtered DataFrame as a parquet
                    target_filename = update_table_name(file)
                    filtered_df.write_csv(
                        f"{target_folder}/{client_name}_{target_filename}.csv"
                        )
                    print(f"✅ DateFrame saved as csv as {target_folder}/{client_name}{target_filename}.csv")
            except Exception as e:
                print(f"Failed Process File {file_path}: {e}")
        

# ------- Function to process Facility Volumetrics Data
def volumetrics_to_target(volumetrics_folder, client_name):
    print(f"🔄 Processing Volumetrics Data for client: {client_name}")
    
    data_processed = []
    files_fails = []
    files_success = []

    for file in os.listdir(volumetrics_folder):
        if file.lower().endswith(".csv"):
            file_path = os.path.join(volumetrics_folder,file)
            print(f"🔄 Processing {file_path}")
            
            try:
                # Polars Read
                df = pl.read_csv(file_path, ignore_errors=True)
                # Filter
                filtered_df = df.filter(pl.col("OperatorName")==client_name)
                # Add Metadata
                filtered_df = filtered_df.with_columns([
                pl.lit(date.today()).alias("effective_from_date"),
                pl.lit(None).alias("effective_to_date"),
                pl.lit("True").alias("is_current")   
                ])

                data_processed.append(filtered_df)

                files_success.append(file)
                
                #Add Comment regarding the sucessfully processed data vs data that was not processed 
            except Exception as e:
                files_fails.append(file)
                print(f"❌ Failed to process {file}: {e}")
                continue

    
    if data_processed:
        combined_data_frame = pl.concat(data_processed, how="diagonal_relaxed")
    else:
        combined_data_frame = pl.DataFrame()

    print("\n✅ Combined DataFrame Created")

    print(f"\n📄 Summary:")
    print(f"🟢 Success ({len(files_success)}):\n{files_success}")
    print(f"🔴 Failed  ({len(files_fails)}):\n{files_fails}")
    return combined_data_frame


## Importing Data to GSC
def stream_to_gsc(source_folder_location, bucket_name, prefix):
    print("🔄 Ingesting File into GCS [Google Cloud Storage]")

    # Upload A: Active (Overwrite latest to BQ ingestion)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    successes, failures = [], []

    for filename in os.listdir(source_folder_location):
        if not filename.lower().endswith(".csv"):
            continue
        
        full_path = os.path.join(source_folder_location, filename)
        if not os.path.isfile(full_path):
            failures.append((filename, "Not a file"))
            continue

        # preserve basename + extension, clean basename and add prefix
        base, ext = os.path.splitext(filename)
        safe_base = update_table_name(base) or base 
        prefix_clean = prefix.rstrip("/")
        blob_name = f"{prefix_clean}/{safe_base}{ext}"
  
        print(f"🔄 Uploading {blob_name} to GCS")

        try:
            blob = bucket.blob(blob_name)  # It simply tells GCS: "I am preparing a slot at this specific path (blob_name) inside this bucket."
            content_type = mimetypes.guess_type(full_path)[0] or "application/octet-stream"
            blob.upload_from_filename(full_path, content_type=content_type) # It reads the data from your local full_path and streams it to Google Cloud.
            successes.append(filename)
            print(f"✅ Uploaded {filename} to {blob_name} with content type {content_type}")
        except Exception as e:
            failures.append((filename, str(e)))
            print(f"❌ Failed to upload {filename}: {e}")   


# Importing Data to BIG QUERY
def stream_to_bq(source_folder_location, project_id, dataset_name):
    print("🔄 Ingesting File into BQ [Big Query]")

    # Initialize BigQuery Client
    bq_client = bigquery.Client()

    successes, failures = [], []

    for filename in os.listdir(source_folder_location):
        if not filename.lower().endswith(".csv"):
            continue
        
        full_path = os.path.join(source_folder_location, filename)
        if not os.path.isfile(full_path):
            failures.append((filename, "Not a file"))
            continue

        table_name = update_table_name(os.path.splitext(filename)[0])
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        print(f"🔄 Uploading {filename} to BQ Table: {table_id}")

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        try:
            with open(full_path, "rb") as source_file:
                load_job = bq_client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config,
                )
            load_job.result()  # Waits for the job to complete.
            successes.append(filename)
            print(f"✅ Uploaded {filename} to BQ Table: {table_id}")
        except GoogleAPICallError as e:
            failures.append((filename, str(e)))
            print(f"❌ Failed to upload {filename} to BQ: {e}")
