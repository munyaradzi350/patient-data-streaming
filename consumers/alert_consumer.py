# consumers/alerts_consumer.py

from kafka import KafkaConsumer
from json import loads
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AlertsConsumer")

# In-memory deduplication (Optional but helps)
seen_alerts = set()

consumer = KafkaConsumer(
    'patient-alerts',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,               
    group_id='alerts-group'                
)

logger.info("🚨 Listening to 'patient-alerts' topic...")

for message in consumer:
    alert_data = message.value
    alert_id = f"{alert_data['Patient_ID']}-{alert_data['Timestamp']}"

    if alert_id not in seen_alerts:
        seen_alerts.add(alert_id)
        logger.warning(f"🚨 ALERT: {alert_data}")
    else:
        logger.debug(f"Duplicate alert skipped: {alert_data}")
