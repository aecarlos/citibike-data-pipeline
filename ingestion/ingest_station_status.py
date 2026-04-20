# ingestion/ingest_station_status.py

import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/es/station_status.json"

DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed" / "station_status"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_data():
    print("🚀 Fetching station_status...")

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    data = r.json()
    stations = data["data"]["stations"]

    df = pd.DataFrame(stations)

    df = df[
        [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning",
            "last_reported",
        ]
    ]

    df["station_id"] = df["station_id"].astype(str)

    df["last_reported"] = pd.to_datetime(df["last_reported"], unit="s", errors="coerce")
    df["ingestion_time"] = datetime.now(timezone.utc)

    return df


def save(df):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_path = PROCESSED_DIR / f"station_status_{ts}.parquet"

    df.to_parquet(
        file_path,
        index=False,
        engine="fastparquet"
    )

    print(f"💾 Saved locally: {file_path}")

    return file_path


def main():
    df = fetch_data()
    save(df)
    print("✅ Done")


if __name__ == "__main__":
    main()
