# consumers/processed_data_consumer.py

from kafka import KafkaConsumer
from json import loads
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProcessedDataConsumer")

consumer = KafkaConsumer(
    'patient-processed-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='processed-data-group'
)

logger.info("✅ Listening to processed-data topic...")
for message in consumer:
    logger.info(f"📥 Processed Patient Data: {message.value}")
