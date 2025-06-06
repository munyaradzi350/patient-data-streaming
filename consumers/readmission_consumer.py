import json
import os
import sys
from kafka import KafkaConsumer

# Add parent path to import local services
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.alert_service import check_readmission_risk
from services.influxdb_service import write_alert_to_influx

# 🔇 Suppress internal Kafka logging
import logging
for noisy_logger in [
    "kafka", "kafka.conn", "kafka.client", "kafka.cluster",
    "kafka.consumer.group", "urllib3.connectionpool"
]:
    logging.getLogger(noisy_logger).setLevel(logging.ERROR)

# Kafka configuration
KAFKA_TOPIC = "patient-raw-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Kafka Consumer
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="readmission-risk-group"
    )
    print("✅ Kafka Consumer connected successfully!")
except Exception as e:
    print(f"❌ Kafka Consumer connection failed: {e}")
    sys.exit(1)

print("📥 Readmission Kafka Consumer started and listening for patients...")

# Consume and process messages
for message in consumer:
    try:
        patient = message.value
        alert = check_readmission_risk(patient)

        if alert:
            fields = {
                "message": "Patient flagged for readmission risk.",
                "days_since_last_visit": alert.get("days_since_last_visit", 0),
                "timestamp": alert["timestamp"]
            }

            write_alert_to_influx("readmission_alerts", alert["Patient_ID"], fields)
            print(f"🚨 Readmission risk triggered for Patient_ID: {alert['Patient_ID']}")
            print(f"✅ InfluxDB write successful for Patient_ID: {alert['Patient_ID']}")

    except Exception as e:
        print(f"❌ Error processing patient data: {e}")
