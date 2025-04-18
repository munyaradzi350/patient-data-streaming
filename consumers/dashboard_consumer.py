# consumers/dashboard_consumer.py

from kafka import KafkaConsumer
from json import loads
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DashboardConsumer")

consumer = KafkaConsumer(
    'patient-dashboard-updates',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='dashboard-group'
)

logger.info("📊 Listening to dashboard topic...")
for message in consumer:
    logger.info(f"📊 Dashboard Update: {message.value}")
