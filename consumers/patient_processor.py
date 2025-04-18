# consumers/patient_processor.py

import sys
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from kafka.errors import KafkaError

# Add parent directory to import services
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.processing_service import process_patient_data
from configs.kafka_config import RAW_DATA_TOPIC, PROCESSED_DATA_TOPIC, ALERTS_TOPIC, DASHBOARD_TOPIC

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PatientProcessor")

# ✅ Kafka Consumer for raw data
consumer = KafkaConsumer(
    RAW_DATA_TOPIC,
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # 👈 Important: read from the beginning
    group_id='patient-processor-group',
    enable_auto_commit=True
)

# ✅ Kafka Producer for sending processed data
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

def start_patient_processing():
    logger.info("🚀 Patient Processor Started and waiting for messages...")

    for message in consumer:
        try:
            patient_data = message.value
            logger.info(f"📥 Received raw patient data: {patient_data}")

            # ✅ Process the patient data
            processed_data, alert_data, dashboard_data = process_patient_data(patient_data)

            # ✅ Send to processed-data topic
            producer.send(PROCESSED_DATA_TOPIC, value=processed_data)
            logger.info("✅ Sent to processed-data topic")

            # ✅ Send to alerts topic 
            if alert_data:
                producer.send(ALERTS_TOPIC, value=alert_data)
                logger.warning("🚨 Sent alert to alerts topic")

            # ✅ Send dashboard update
            producer.send(DASHBOARD_TOPIC, value=dashboard_data)
            logger.info("📊 Sent update to dashboard topic")

            producer.flush()  # Ensure all messages are sent

        except KafkaError as ke:
            logger.error(f"❌ Kafka error: {ke}")
        except Exception as e:
            logger.error(f"❌ General error: {e}")

if __name__ == "__main__":
    start_patient_processing()
