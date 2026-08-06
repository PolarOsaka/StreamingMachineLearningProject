import json
import logging
from kafka import KafkaConsumer
from streaming.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
)
from config.logging_config import configure_logging

logger = logging.getLogger(__name__)

def create_consumer() -> KafkaConsumer:
    """
    Create and return a Kafka consumer.
    """

    logger.info("Connecting to Kafka broker...")

    consumer = KafkaConsumer(
        TOPIC_NAME, # Subscribe ke topik yang sama dengan producer
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
        auto_offset_reset="earliest", # Supaya pesan yg dikirim producer saat consumer belum hidup tetap dibaca
        # Kalau diset latest, pesan yang dibaca dimulai saat consumer baru mulai hidup
        enable_auto_commit=True, # Supaya pesan yang sudah dibaca diberi tanda offset
    )

    logger.info("Successfully connected to Kafka.")

    return consumer

def consume_events(
    consumer: KafkaConsumer,
) -> None:
    """
    Continuously consume events from Kafka.
    """

    logger.info("Waiting for events...")

    for message in consumer:

        event = message.value

        logger.info(
            "Received | visitor=%s | event=%s | item=%s",
            event["visitorid"],
            event["event"],
            event["itemid"],
        )

def main():

    consumer = create_consumer()

    try:

        consume_events(
            consumer,
        )

    finally:

        consumer.close()

        logger.info("Consumer closed.")

if __name__ == "__main__":
    configure_logging()
    main()