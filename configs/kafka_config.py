# kafka_config.py

KAFKA_BROKER = "localhost:9092" 

# Kafka topics
RAW_DATA_TOPIC = "patient-raw-data"
PROCESSED_DATA_TOPIC = "patient-processed-data"
ALERTS_TOPIC = "patient-alerts"
DASHBOARD_TOPIC = "patient-dashboard-updates"

# Other configurations
ACKS = "all"  # Ensures Kafka confirms message reception
RETRIES = 3  # Retry sending if Kafka is unavailable
BATCH_SIZE = 16384  # Optimize message batching
