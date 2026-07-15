import logging
import pandas as pd
from config.settings import EVENTS_FILE

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "timestamp",
    "visitorid",
    "event",
    "itemid",
    "transactionid",
}

def _validate_events(df: pd.DataFrame) -> None: # tanda _fungsi adalah konvensi yg menandakan bahwa fungsi bersifat local, hanya diakses dalam internal module
    """
    Validate the RetailRocket events dataset.

    Args:
        df (pd.DataFrame): Events dataset.

    Raises:
        ValueError: If the dataset is empty.
        ValueError: If required columns are missing.
    """

    if df.empty:
        raise ValueError("The events dataset is empty.")
    
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )
    
    logger.info("Dataset validation successful.")

def read_events() -> pd.DataFrame:
    """
    Read RetailRocket events dataset from the raw layer.

    Returns:
        pd.DataFrame: Raw events dataset.

    Raises:
        FileNotFoundError: If events.csv does not exist.
        ValueError: If the dataset is empty.
    """

    logger.info(f"Reading dataset from {EVENTS_FILE}")

    if not EVENTS_FILE.exists():
        raise FileNotFoundError(f"Dataset not found: {EVENTS_FILE}")
    
    df = pd.read_csv(EVENTS_FILE, low_memory=False)

    _validate_events(df)

    logger.info(
        "Loaded %d rows and %d columns.",
        len(df),
        len(df.columns),
    )

    return df