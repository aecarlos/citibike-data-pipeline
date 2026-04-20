import requests
import pandas as pd
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# FETCH DATA
# -----------------------------
def fetch_stations():
    print("Fetching stations...")

    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    stations = data["data"]["stations"]

    df = pd.DataFrame(stations)

    return df


# -----------------------------
# CLEAN
# -----------------------------
def clean(df):
    df = df.copy()

    df = df[[
        "station_id",
        "name",
        "lat",
        "lon",
        "capacity"
    ]]

    df.columns = [
        "station_id",
        "station_name",
        "lat",
        "lon",
        "capacity"
    ]

    return df


# -----------------------------
# SAVE
# -----------------------------
def save(df):
    output_path = PROCESSED_DIR / "stations.parquet"

    df.to_parquet(
        output_path,
        index=False,
        engine="fastparquet"
    )

    print(f"💾 Saved: {output_path}")


# -----------------------------
# MAIN
# -----------------------------
def main():
    df = fetch_stations()
    df = clean(df)
    save(df)

    print("🎉 Stations ingestion done")


if __name__ == "__main__":
    main()
