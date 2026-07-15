import logging
import pandas as pd
from pathlib import Path
from config.settings import (
    SILVER_EVENTS_FILE,
    BRONZE_EVENTS_FILE
)

logger = logging.getLogger(__name__)

def _clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning and standardization to the RetailRocket
    events dataset.

    Args:
        df (pd.DataFrame): Bronze events dataset.

    Returns:
        pd.DataFrame: Cleaned events dataset.
    """
    clean_df = df.copy()

    clean_df["timestamp"] = pd.to_datetime(
        clean_df["timestamp"],
        unit="ms",
    )
    logger.info("Transformed timestamp")

    clean_df = clean_df.drop_duplicates()
    logger.info(
        "Removed %d duplicate rows.",
        len(df) - len(clean_df),
    )

    return clean_df

def create_silver_layer(bronze_file: Path) -> Path:
    """Processes the Bronze events dataset and saves the cleaned data into the

    Silver layer.

    This function reads a Parquet file from the Bronze layer, applies necessary
    cleaning procedures (converting timestamps, removing duplicates), ensures the
    target directory exists, and stores the output as a Silver Parquet file.

    Args:
        bronze_file (Path): Path to the source Bronze Parquet file.

    Returns:
        Path: Path to the generated Silver Parquet file.

    Raises:
        FileNotFoundError: If the provided bronze_file does not exist.
        Exception: For any other unexpected errors during processing.
    """
    logger.info("Starting Silver layer creation process.")
    logger.info("Reading Bronze file from: %s", bronze_file)

    if not bronze_file.exists():
        raise FileNotFoundError(f"Bronze file missing: {bronze_file}")

    try:
        # Membaca data dari Bronze layer
        df = pd.read_parquet(bronze_file)

        # Proses pembersihan data
        clean_df = _clean_events(df)

        # Memastikan direktori target sudah siap
        SILVER_EVENTS_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Menyimpan ke Silver layer
        logger.info("Saving cleaned data to Silver layer...")
        clean_df.to_parquet(
            SILVER_EVENTS_FILE,
            index=False,
        )
        logger.info(
            "Successfully created Silver layer dataset at: %s",
            SILVER_EVENTS_FILE,
        )

        return SILVER_EVENTS_FILE

    except Exception as e:
        logger.error(
            "Failed to create Silver layer due to an error: %s",
            str(e),
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    create_silver_layer(BRONZE_EVENTS_FILE)