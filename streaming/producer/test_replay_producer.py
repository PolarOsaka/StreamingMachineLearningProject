import json
import logging
from kafka import KafkaProducer
from streaming.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
)
from config.logging_config import configure_logging

logger = logging.getLogger(__name__)

def create_producer() -> KafkaProducer: # Membuat koneksi ke Kafka
    """
    Create and return a Kafka producer.
    """

    logger.info("Connecting to Kafka broker...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    logger.info("Successfully connected to Kafka.")

    return producer

def send_test_message(producer: KafkaProducer) -> None: # Mengirim event dummy
    """
    Send a single dummy RetailRocket event.
    """

    event = {
        "visitorid": 1,
        "event": "view",
        "itemid": 100,
        "timestamp": 0,
    }

    logger.info("Sending test event...")

    producer.send(
        TOPIC_NAME,
        event,
    )

    logger.info("Test event sent.")

def main(): # Mengorkestrasi kedua fungsi di atas
    producer = create_producer()

    try: 
        send_test_message(producer)
        producer.flush() # "memaksa" mengirim semua data yang masih tertahan di memori (buffer) komputer kita langsung ke server Kafka (broker), 
        # sekaligus memaksa program untuk menunggu sampai proses pengiriman tersebut benar-benar selesai.

    finally: # Supaya kalaupun send dan flush producer error, producer tetap di close sehingga koneksi tidak nyala terus di background dan tidak memakan memori atau slot koneksi
        producer.close()
        logger.info("Producer closed.")

if __name__ == "__main__":
    configure_logging()
    main()