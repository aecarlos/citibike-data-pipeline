from google.cloud import bigquery

CREDENTIALS_PATH = "configs/gcp_credentials.json"

BUCKET_NAME = "bike-data-raw-12345"

# ✔ IMPORTANTE: wildcard por nombre de archivo
GCS_URI = "gs://bike-data-raw-12345/citibike/processed/station_status_*.parquet"

TABLE_ID = "bike_data.raw_citibike_station_status"


client = bigquery.Client.from_service_account_json(
    CREDENTIALS_PATH
)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition="WRITE_APPEND",
    autodetect=True
)


def load_station_status():

    print("🚀 Loading station_status to BigQuery...")

    job = client.load_table_from_uri(
        GCS_URI,
        TABLE_ID,
        job_config=job_config
    )

    job.result()

    print(f"✅ Loaded → {TABLE_ID}")


if __name__ == "__main__":
    load_station_status()
