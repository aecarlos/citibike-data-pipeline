import requests
import zipfile
import pandas as pd
import gc
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone

# -----------------------------
# CONFIG (COMPATIBLE CON TU SETUP)
# -----------------------------
BASE_URL = "https://s3.amazonaws.com/tripdata"

# 👇 MISMA ESTRUCTURA QUE YA TENÍAS
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LOOKBACK_MONTHS = 3  # solo revisa últimos meses


# -----------------------------
# GENERATE RECENT MONTHS URLS
# -----------------------------
def generate_recent_urls(lookback_months):
    urls = []

    now = datetime.now()

    for i in range(lookback_months):
        year = now.year
        month = now.month - i

        if month <= 0:
            month += 12
            year -= 1

        ym = f"{year}{month:02d}"
        url = f"{BASE_URL}/{ym}-citibike-tripdata.zip"
        urls.append(url)

    return urls


# -----------------------------
# DOWNLOAD
# -----------------------------
def download_zip(url):
    try:
        print(f"⬇️ Downloading: {url}")
        r = requests.get(url, timeout=60)

        if r.status_code != 200:
            print(f"⏭️ Not available yet: {url}")
            return None

        return BytesIO(r.content)

    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return None


# -----------------------------
# EXTRACT
# -----------------------------
def extract_csvs(zip_bytes):
    dfs = []

    with zipfile.ZipFile(zip_bytes) as z:
        for file in z.namelist():
            if file.endswith(".csv"):
                with z.open(file) as f:
                    dfs.append(pd.read_csv(f, low_memory=False))

    return dfs


# -----------------------------
# CLEAN
# -----------------------------
def clean_dataframe(df):
    id_cols = ["ride_id", "start_station_id", "end_station_id"]

    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


# -----------------------------
# PROCESS
# -----------------------------
def process_dataset(url):
    zip_bytes = download_zip(url)
    if not zip_bytes:
        return None, None

    dfs = extract_csvs(zip_bytes)

    if not dfs:
        print(f"⚠️ Empty ZIP: {url}")
        return None, None

    df = pd.concat(dfs, ignore_index=True)
    df = clean_dataframe(df)

    return df, url


# -----------------------------
# SAVE (FULL COMPATIBLE)
# -----------------------------
def save_processed(df, url):
    # 👇 MISMA LOGICA QUE YA TENÍAS
    filename = url.split("/")[-1].replace(".zip", ".parquet")
    output_path = PROCESSED_DIR / filename

    # idempotencia (NO rompe nada)
    if output_path.exists():
        print(f"⏭️ Already exists: {filename}")
        return

    # metadata (seguro para downstream)
    df["source_file"] = filename
    df["ingestion_time"] = datetime.now(timezone.utc)

    df.to_parquet(output_path, index=False)

    print(f"💾 Saved: {output_path}")

    del df
    gc.collect()


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("🚀 Citibike ingestion (COMPAT MODE)")

    urls = generate_recent_urls(LOOKBACK_MONTHS)

    print(f"🔍 Checking {len(urls)} recent datasets")

    for url in urls:
        df, url = process_dataset(url)

        if df is not None:
            save_processed(df, url)

            del df
            gc.collect()

    print("✅ Done")


if __name__ == "__main__":
    main()
