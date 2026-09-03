import pandas as pd
import time
import json
import logging
from kafka import KafkaProducer
from streaming.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    REPLAY_DELAY
)
from config.logging_config import configure_logging
from config.settings import SILVER_EVENTS_FILE

logger = logging.getLogger(__name__)

def create_producer() -> KafkaProducer: # Membuat koneksi ke Kafka
    """
    Create and return a Kafka producer.
    """

    logger.info("Connecting to Kafka broker...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            default=str, # Jika value mengandung tipe data yang tidak dikenal json seperti UUID, maka dibuat jadi str
        ).encode("utf-8"),
    )

    logger.info("Successfully connected to Kafka.")

    return producer

def load_events() -> pd.DataFrame:
    """
    Load cleaned RetailRocket events from the Silver layer.

    Returns:
        pd.DataFrame: Silver events.
    """

    logger.info("Loading Silver dataset...")

    df = pd.read_parquet(SILVER_EVENTS_FILE)

    logger.info("Loaded %d events.", len(df))

    return df

def send_event(
    producer: KafkaProducer,
    event: dict,
) -> None:
    """
    Send a single event to Kafka.
    """

    producer.send( # Sifat komunikasi ini adalah asinkron
        TOPIC_NAME,
        event,
    )

    # Bisa pakai ini supaya sinkron
    # future = producer.send(
    #     TOPIC_NAME,
    #     event,
    # )

    # future.get(timeout=10) # Akan menunggu ACK

    logger.info(
        "Sent event: visitor=%s event=%s item=%s",
        event["visitorid"],
        event["event"],
        event["itemid"],
    )

def replay_events(
    producer: KafkaProducer,
    events_df: pd.DataFrame,
) -> None:
    """
    Replay all RetailRocket events to Kafka.
    """

    logger.info(
        "Starting replay of %d events...",
        len(events_df),
    )

    for row in events_df.itertuples(index=False):

        event = row._asdict()

        send_event( 
            producer,
            event,
        ) 

        time.sleep(REPLAY_DELAY)

    logger.info("Replay completed.")

def main(): # Mengorkestrasi kedua fungsi di atas
    producer = create_producer()

    try: 
        events_df = load_events()
        replay_events(
            producer,
            events_df,
        )
        producer.flush() # "memaksa" mengirim semua data yang masih tertahan di memori (buffer) komputer kita langsung ke server Kafka (broker), 
        # sekaligus memaksa program untuk menunggu sampai proses pengiriman tersebut benar-benar selesai.

    finally: # Supaya kalaupun send dan flush producer error, producer tetap di close sehingga koneksi tidak nyala terus di background dan tidak memakan memori atau slot koneksi
        producer.close()
        logger.info("Producer closed.")

if __name__ == "__main__":
    configure_logging()
    main()