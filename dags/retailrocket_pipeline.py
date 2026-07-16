from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
from src.bronze import create_bronze_layer # Jangan lupa setup volumes di docker-compose.yml supaya docker airflow bisa akses modul di src
from src.silver import create_silver_layer

import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id="retailrocket_pipeline", 
    start_date=datetime(2026, 7, 15), 
    schedule=None, 
    catchup=False, 
    tags=["retailrocket", "etl", "batch"],
    default_args= {
        "owner":"Polar",
        "retries":2,
        "retry_delay":timedelta(seconds=15),
    },
    description = "RetailRocket ETL Pipeline",
    doc_md="""
        # RetailRocket ETL Pipeline

        This DAG performs a simple batch ETL pipeline.

        ## Workflow

        1. Read RetailRocket events
        2. Create Bronze layer
        3. Create Silver layer

        ## Output

        - data/bronze/events.parquet
        - data/silver/events.parquet
        """
)

def retailrocket_pipeline():
    @task
    def start():
        logger.info("Starting ETL...")
    @task
    def bronze_layer():
        return str(create_bronze_layer())
    @task
    def silver_layer(bronze_file):
        bronze_path = Path(bronze_file)
        return str(create_silver_layer(bronze_path)) # Tapi output adalah string ya
    @task
    def finish():
        logger.info("ETL finished!")

    # Dependency
    start() >> silver_layer(bronze_layer()) >> finish()

retailrocket_pipeline()

# Untuk jalankan di command, run: docker compose exec airflow-scheduler airflow dags trigger retailrocket_pipeline