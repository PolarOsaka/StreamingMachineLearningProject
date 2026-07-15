# Kode ini akan membaca file events.csv, menyimpannya dalam bentuk parquet, lalu mengembalikan path menuju data parquet
import logging
from pathlib import Path
from config.settings import BRONZE_EVENTS_FILE
from src.extract import read_events

logger = logging.getLogger(__name__)

def create_bronze_layer() -> Path:
    """
    Create the Bronze layer by converting the raw RetailRocket
    events dataset from CSV to Parquet.

    Returns:
        Path: Path to the generated Bronze Parquet file.
    """
    
    df = read_events()

    logger.info("Saving bronze dataset")
    BRONZE_EVENTS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    df.to_parquet(BRONZE_EVENTS_FILE, index=False)
    logger.info("Bronze dataset saved to %s", BRONZE_EVENTS_FILE)

    return BRONZE_EVENTS_FILE

if __name__ == "__main__": # Kalau ada ini, artinya fungsi ini bisa di-run di terminal tanpa harus dipanggil fungsi lain
    create_bronze_layer()