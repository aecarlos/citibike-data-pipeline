from google.cloud import bigquery


def load_csv_from_gcs_to_bq(uri: str, destination_table_id: str) -> None:
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(uri, destination_table_id, job_config=job_config)
    load_job.result()

    destination_table = client.get_table(destination_table_id)
    print(f'Loaded {destination_table.num_rows} rows into {destination_table_id}')
