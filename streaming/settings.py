# Alamat atau entry point agar aplikasi bisa terhubung ke Kafka
KAFKA_BOOTSTRAP_SERVERS="kafka:9092" 

# Nama channel atau topic tempat data disimpan dalam Kafka
TOPIC_NAME="retailrocket-events"

# Jeda waktu dalam proses pembacaan data (satuan detik)
REPLAY_DELAY=10