# consumers/influxdb_consumer.py

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from json import loads
import logging
import os

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("InfluxDBConsumer")

# 🔌 Kafka config
KAFKA_BROKER = 'localhost:9092'
DASHBOARD_TOPIC = 'patient-dashboard-updates'

# 🔌 InfluxDB config (Update these to your actual config)
INFLUXDB_URL = "http://127.0.0.1:8086"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "AW_pSzi-vK51tPBoTLkUSOkG548UECZOO2NyUbQEUNCy6qyjPcm558P2OzKKBpzM9vfJ3gV9eiJloKKptjzhPg==")
INFLUXDB_ORG = "Data Engineering"
INFLUXDB_BUCKET = "patient-monitoring"

# Kafka Consumer
consumer = KafkaConsumer(
    DASHBOARD_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: loads(x.decode("utf-8")),
    auto_offset_reset='latest',
    group_id='influxdb-consumer-group'
)

# InfluxDB Client
influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def write_to_influx(patient_metrics: dict):
    try:
        point = (
            Point("patient_vitals")
            .tag("Patient_ID", patient_metrics["Patient_ID"])
            .field("Heart_Rate", float(patient_metrics.get("Heart_Rate", 0)))
            .field("Temperature", float(patient_metrics.get("Temperature", 0)))
            .field("Blood_Sugar_Level", float(patient_metrics.get("Blood_Sugar_Level", 0)))
            .field("BMI", float(patient_metrics.get("BMI", 0)))
            .field("Blood_Pressure", patient_metrics.get("Blood_Pressure", ""))
            .time(patient_metrics.get("timestamp"))  # ISO format time
        )

        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info(f"📡 Written to InfluxDB: {patient_metrics['Patient_ID']}")
    except Exception as e:
        logger.error(f"❌ Failed to write to InfluxDB: {e}")

def start_influxdb_consumer():
    logger.info("🚀 InfluxDB Consumer started. Listening for dashboard data...")
    for message in consumer:
        patient_metrics = message.value
        logger.info(f"📥 Received dashboard data: {patient_metrics}")
        write_to_influx(patient_metrics)

if __name__ == "__main__":
    try:
        start_influxdb_consumer()
    except KeyboardInterrupt:
        logger.info("🛑 Stopping InfluxDB consumer...")
    finally:
        consumer.close()
        influx_client.close()
