import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Robust path configuration
BASE_DIR = Path(__file__).resolve().parent
INGESTION_DIR = BASE_DIR / "ingestion"
DBT_DIR = BASE_DIR / "dbt"

env_path = BASE_DIR / "configs" / ".env"
load_dotenv(dotenv_path=env_path)

def run_python_script(script_name):
    """Executes a Python script located within the ingestion directory."""
    script_path = INGESTION_DIR / script_name
    print(f"🚀 Executing: {script_name}...")

    result = subprocess.run([sys.executable, str(script_path)], capture_output=False)

    if result.returncode != 0:
        print(f"❌ Critical error in {script_name}. Aborting pipeline.")
        sys.exit(1)

def run_dbt(select_models=None):
    """Triggers dbt transformations. Optional: select specific models."""
    print(f"🏗️  Starting dbt transformations...")

    PROFILES_DIR = Path.home() / ".dbt"
    dbt_command = [
        "dbt", "run",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(PROFILES_DIR)
    ]

    if select_models:
        dbt_command += ["--select", select_models]
        print(f"🎯 Targeting models: {select_models}")

    result = subprocess.run(dbt_command, capture_output=False)

    if result.returncode == 0:
        print("\n🏆 TASK COMPLETED SUCCESSFULLY! 🏆")
    else:
        print("\n⚠️ dbt finished with errors.")

if __name__ == "__main__":
    # Check for command line arguments
    # Usage: python run_pipeline.py --only-status
    only_status = "--only-status" in sys.argv

    if only_status:
        print("⏱️ RUNNING HOURLY STATUS UPDATE (LIGHT MODE)")
        scripts = [
            "ingest_station_status.py",
            "upload_to_gcs.py",
            "load_station_status_to_bq.py"
        ]
        # We only run the status-related models in dbt
        dbt_filter = "mart_station_status" # Replace with your actual model name
    else:
        print("🐘 RUNNING FULL DAILY PIPELINE (HEAVY MODE)")
        scripts = [
            "ingest_citibike.py",
            "ingest_stations.py",
            "ingest_station_status.py",
            "upload_to_gcs.py",
            "load_to_bq.py",
            "load_stations_to_bq.py",
            "load_station_status_to_bq.py"
        ]
        dbt_filter = None # Run everything

    print("--- CITIBIKE DATA PIPELINE ---\n")

    for script in scripts:
        run_python_script(script)
        print("-" * 30)

    run_dbt(select_models=dbt_filter)
