import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# --- CONFIGURATION & PATHS ---
BASE_DIR = Path(__file__).resolve().parent
INGESTION_DIR = BASE_DIR / "ingestion"
DBT_DIR = BASE_DIR / "dbt"

# Load environment variables from the config folder
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
    """
    Triggers dbt transformations with dynamic profile detection.
    Works seamlessly on both Local (project-level) and VM (system-level).
    """
    print(f"🏗️  Starting dbt transformations...")

    # --- DYNAMIC PROFILE DETECTION ---
    # 1. Try to find profiles.yml inside the project first (Local portability)
    project_profile = DBT_DIR / "profiles.yml"

    if project_profile.exists():
        PROFILES_DIR = DBT_DIR
        print(f"📂 Mode: Using PROJECT-LEVEL profile found in {PROFILES_DIR}")
    else:
        # 2. Fallback to the system hidden folder (Default VM behavior)
        PROFILES_DIR = Path.home() / ".dbt"
        print(f"🏠 Mode: Project profile not found. Using SYSTEM-LEVEL at {PROFILES_DIR}")

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
        # Targeting specific dbt model for status updates
        dbt_filter = "+mart_station_realtime_metrics"
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
        dbt_filter = None # Run the whole project

    print("\n--- CITIBIKE DATA PIPELINE ---\n")

    # Step 1: Run Ingestion Scripts
    for script in scripts:
        run_python_script(script)
        print("-" * 30)

    # Step 2: Run dbt Transformations
    run_dbt(select_models=dbt_filter)
