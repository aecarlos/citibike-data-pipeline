import os
from pathlib import Path
from google.cloud import storage


def upload_file_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    source_path = Path(source_file_path)
    if not source_path.exists():
        raise FileNotFoundError(f'Local file not found: {source_file_path}')

    blob.upload_from_filename(str(source_path))
    print(f'Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}')


def get_gcs_uri(bucket_name: str, destination_blob_name: str) -> str:
    return f'gs://{bucket_name}/{destination_blob_name}'
