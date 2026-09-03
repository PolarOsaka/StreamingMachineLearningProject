import logging

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink
.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from streaming.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
)

def create_environment():
    """
    Create Flink streaming execution environment.
    """

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1) # Artinya, seluruh alur program (mulai dari membaca Kafka, memproses data, hingga menulis ke tujuan) hanya akan dijalankan oleh 1 thread / 1 subtask saja secara urut (sequential)
    # Karena task manager punya 2 task slot (sesuai di .yaml), maka bila eksekusi sekuensial, hanya 1 task slot yang digunakan oleh si task manager. Slot satunya lagi akan idle
    # Andai kita punya 2 task manager dengan 2 task slots masing2. Maka ada total 4 task slots. Dgn paralelism 2, maka hanya 1 task slot dari masing2 task manager yg kepakai (merata)
    # Jika total 4 task slots tapi paralelism 5, berarti ada 5 threads, maka error
    return env

def create_kafka_source():
    """
    Create Kafka source for RetailRocket events.
    """

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(TOPIC_NAME)
        .set_group_id("retailrocket-flink") # Kafka menggunakan ID ini untuk mengelompokkan konsumen dan mencatat posisi pembacaan (offset).
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest() # Menentukan posisi awal pembacaan. earliest() menginstruksikan Flink untuk membaca pesan dari posisi paling awal (offset 0) jika belum ada offset yang tercatat untuk group_id ini.
        )
        .set_value_only_deserializer(
            SimpleStringSchema() # Mengonversi pesan mentah (bytes) dari value Kafka menjadi format teks biasa (String), mengabaikan key atau header dari pesan Kafka.
        )
        .build()
    )

    return source


def main():

    env = create_environment()

    source = create_kafka_source()

    events = env.from_source(
        source,
        watermark_strategy=None, # Watermark strategy diperlukan untuk membaca timestamp dari teks json. Karena timestamp berbentuk str disini, maka watermark strategy tidak bisa digunakan dulu
        # Watermark strategy diperlukan untuk mengukur perjalanan Event Time (waktu asli saat kejadian terjadi di aplikasi). 
        # Jika pipeline Anda memiliki operasi berbasis jendela (windowing), Flink yang tanpa watermark akan secara otomatis mengandalkan Processing Time (waktu mesin/server Flink saat memproses data tersebut).
        source_name="retailrocket-kafka-source",
    )

    events.print()

    env.execute(
        "RetailRocket Kafka Consumer"
    )

if __name__ == "__main__":
    main()