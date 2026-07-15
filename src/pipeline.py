# Jika nantinya sudah menggunakan Airflow, maka pipeline.py ini tidak akan digunakan

import logging
from src.bronze import create_bronze_layer
from src.silver import create_silver_layer
from config.logging_config import configure_logging

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Run the RetailRocket ETL pipeline.

    Workflow:
        Raw CSV
            ↓
        Bronze Layer
            ↓
        Silver Layer
    """

    logger.info("Starting ETL pipeline...")

    bronze_file = create_bronze_layer()

    silver_file = create_silver_layer(bronze_file)

    logger.info(
        "Pipeline completed successfully. Silver dataset available at %s",
        silver_file,
    )
    
if __name__ == "__main__":
    configure_logging()
    run_pipeline()