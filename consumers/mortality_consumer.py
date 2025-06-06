import json
import os
import sys
from kafka import KafkaConsumer
from datetime import datetime

# Add parent directory to path for service imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Suppress Kafka internal logs
import logging
for noisy_logger in ["kafka", "kafka.conn", "kafka.client", "kafka.cluster", "kafka.consumer.group", "urllib3.connectionpool"]:
    logging.getLogger(noisy_logger).setLevel(logging.ERROR)

from services.alert_service import check_mortality_risk
from services.influxdb_service import write_alert_to_influx

# Kafka Config
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
        group_id="mortality-risk-group"
    )
    print("✅ Kafka Consumer connected successfully!")
except Exception as e:
    print(f"❌ Kafka Consumer connection failed: {e}")
    sys.exit(1)

print("📥 Mortality Kafka Consumer started and listening for patients...")

# Consume messages
for message in consumer:
    patient = message.value

    if check_mortality_risk(patient):
        patient_id = patient.get("Patient_ID")
        age = patient.get("Age", 0)
        heart_rate = patient.get("Heart_Rate", 0)
        cholesterol = patient.get("Cholesterol_Level", "").lower()
        genomic_info = patient.get("Genomic_Info", "").lower()

        # Parse blood pressure
        systolic, diastolic = 0, 0
        try:
            systolic, diastolic = map(int, patient.get("Blood_Pressure", "0/0").split("/"))
        except Exception as e:
            print(f"⚠️ Invalid Blood_Pressure format for Patient_ID {patient_id}: {e}")

        # Flags
        cholesterol_flag = cholesterol in ["high", "borderline"]
        genomic_flag = genomic_info in ["risk", "positive", "predisposed"]

        fields = {
            "risk": "mortality",
            "details": "Patient flagged for mortality risk.",
            "age": age,
            "heart_rate": heart_rate,
            "systolic": systolic,
            "diastolic": diastolic,
            "cholesterol_flag": cholesterol_flag,
            "genomic_flag": genomic_flag
        }

        print(f"🚨 Mortality risk triggered for Patient_ID: {patient_id}")

        try:
            write_alert_to_influx("mortality_alerts", patient_id, fields)
            print(f"✅ InfluxDB write successful for Patient_ID: {patient_id}")
        except Exception as e:
            print(f"❌ Failed to write to InfluxDB for Patient_ID: {patient_id} | Error: {e}")
