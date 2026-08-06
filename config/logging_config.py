# Jika nantinya sudah menggunakan Airflow,
# Maka logging basic config ini sudah tidak dibutuhkan
# Cukup lakukkan logger.info() di modul src dan Airflow akan do the rest

import logging
from config.settings import LOG_DIR, LOG_FILE

def configure_logging() -> None:

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    logging.basicConfig(
        level=logging.INFO, #Level akan berupa info, warning, error, critical
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ],
        force=True # Supaya kalau ada config log lain, maka config log yang di force ini lah yg tetap dipakai
    )

