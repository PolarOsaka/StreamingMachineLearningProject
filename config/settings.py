from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Path(__file__).resolve() akan mengambil path dari file saat ini, yakni path dari file settings.py ini
# Path tersebut adalah /workspaces/StreamingMachineLearningProject/src/settings.py
# Lalu parent.parent akan mengambil ini saja /workspaces/StreamingMachineLearningProject

DATA_DIR = PROJECT_ROOT / "data"
# Sehingga path data menjadi /workspaces/StreamingMachineLearningProject/data

RAW_DIR = DATA_DIR / "raw"

EVENTS_FILE = RAW_DIR / "events.csv" 

BRONZE_DIR = DATA_DIR / "bronze"

BRONZE_EVENTS_FILE = BRONZE_DIR / "events.parquet"

SILVER_DIR = DATA_DIR / "silver"

SILVER_EVENTS_FILE = SILVER_DIR / "cleaned_events.parquet"

LOG_DIR = PROJECT_ROOT / "logs"

LOG_FILE = LOG_DIR / "pipeline.log"