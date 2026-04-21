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

    # -----------------------------
    # 1. SELECT COLUMNS (safe)
    # -----------------------------
    expected_cols = [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
    ]

    df = df[expected_cols].copy()

    # -----------------------------
    # 2. CLEAN station_id
    # -----------------------------
    df["station_id"] = df["station_id"].astype(str)

    # -----------------------------
    # 3. FIX last_reported (CRITICAL FIX)
    # -----------------------------

    df["last_reported"] = pd.to_numeric(df["last_reported"], errors="coerce")

    df["last_reported"] = pd.to_datetime(
        df["last_reported"],
        unit="ns",
        errors="coerce",
        utc=True
    )

    # Remove invalid epoch values (very old / garbage data)
    cutoff = pd.Timestamp("2020-01-01", tz="UTC")
    df.loc[df["last_reported"] < cutoff, "last_reported"] = pd.NaT

    # -----------------------------
    # 4. INGESTION TIMESTAMP
    # -----------------------------
    df["ingestion_time"] = datetime.now(timezone.utc)

    # -----------------------------
    # 5. DEBUG OPTIONAL (can remove later)
    # -----------------------------
    print(f"📊 Rows: {len(df)}")
    print(f"❗ Null last_reported: {df['last_reported'].isna().sum()}")

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
