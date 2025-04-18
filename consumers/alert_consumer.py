# consumers/alerts_consumer.py

from kafka import KafkaConsumer
from json import loads
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AlertsConsumer")

consumer = KafkaConsumer(
    'patient-alerts',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='alerts-group'
)

logger.info("🚨 Listening to alerts topic...")
for message in consumer:
    logger.warning(f"🚨 ALERT: {message.value}")
