from google.cloud import storage
from pathlib import Path
import time

BUCKET_NAME = "bike-data-raw-12345"

# 🔥 IMPORTANTE: ahora separas bien station_status
LOCAL_DIR = Path("data/processed")

GCS_PREFIX = "citibike/processed"

UPLOAD_TIMEOUT = 300
MAX_RETRIES = 3


client = storage.Client.from_service_account_json(
    "configs/gcp_credentials.json"
)

bucket = client.bucket(BUCKET_NAME)


def file_exists(blob_name):
    blob = bucket.blob(blob_name)
    return blob.exists(client)


def upload_file(file_path: Path):

    blob_name = f"{GCS_PREFIX}/{file_path.name}"

    if file_exists(blob_name):
        print(f"⏭️ Skipping: {file_path.name}")
        return

    blob = bucket.blob(blob_name)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"⬆️ Uploading: {file_path.name}")

            blob.upload_from_filename(str(file_path), timeout=UPLOAD_TIMEOUT)

            print(f"✅ Uploaded: {file_path.name}")
            return

        except Exception as e:
            print(f"⚠️ Error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)


def main():

    files = sorted(list(LOCAL_DIR.rglob("*.parquet")))

    if not files:
        raise Exception("No parquet found")

    for file_path in files:
        upload_file(file_path)

    print("🎉 DONE")


if __name__ == "__main__":
    main()
