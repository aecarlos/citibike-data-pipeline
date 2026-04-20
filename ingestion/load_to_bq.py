from google.cloud import bigquery

client = bigquery.Client.from_service_account_json(
    "configs/gcp_credentials.json"
)

table_id = "bike_data.raw_citibike_trips"

uri = "gs://bike-data-raw-12345/citibike/processed/20*.parquet"
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition="WRITE_TRUNCATE",
)

job = client.load_table_from_uri(uri, table_id, job_config=job_config)

print("Loading to BigQuery...")

job.result()

print("Done → BigQuery loaded")
