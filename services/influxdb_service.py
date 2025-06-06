import logging
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# InfluxDB Configuration
INFLUXDB_URL = "http://127.0.0.1:8086"
INFLUXDB_TOKEN = "AW_pSzi-vK51tPBoTLkUSOkG548UECZOO2NyUbQEUNCy6qyjPcm558P2OzKKBpzM9vfJ3gV9eiJloKKptjzhPg=="
INFLUXDB_ORG = "Data Engineering"
INFLUXDB_BUCKET = "patient-monitoring"

# Initialize client and write API
client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

def write_alert_to_influx(measurement, patient_id, fields):
    from influxdb_client import Point
    from datetime import datetime

    point = Point(measurement).tag("Patient_ID", patient_id).time(datetime.utcnow())

    # Add each key-value in fields as a field in Influx
    for key, value in fields.items():
        point = point.field(key, value)

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logging.info(f"✅ Alert written to InfluxDB: {measurement} for {patient_id}")
    except Exception as e:
        logging.error(f"❌ Failed to write to InfluxDB: {e}")
