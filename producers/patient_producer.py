import json
import time
from kafka import KafkaProducer
from pymongo import MongoClient
from kafka.errors import KafkaError

# ✅ Kafka Configuration
KAFKA_BROKER = "localhost:9092"
RAW_DATA_TOPIC = "patient-raw-data"

# ✅ MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "hospital_db"
MONGO_COLLECTION = "patients"

# ✅ Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("✅ Kafka Producer connected successfully!")
except Exception as e:
    print(f"❌ Kafka connection failed: {e}")
    exit(1)

# ✅ Connect to MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    patient_collection = db[MONGO_COLLECTION]
    print("✅ Connected to MongoDB successfully!")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
    exit(1)

def fetch_latest_patient():
    """Fetch the latest patient record from MongoDB."""
    try:
        patient = patient_collection.find_one({}, sort=[("_id", -1)])
        if patient:
            patient["_id"] = str(patient["_id"])  # Convert ObjectId to string
            return patient
        else:
            return None
    except Exception as e:
        print(f"❌ Error fetching patient data: {e}")
        return None

def produce_patient_data():
    """Continuously stream patient data from MongoDB to Kafka."""
    print(f"🚀 Streaming patient data to Kafka topic: {RAW_DATA_TOPIC}")

    while True:
        patient_data = fetch_latest_patient()

        if patient_data:
            try:
                future = producer.send(RAW_DATA_TOPIC, patient_data)
                future.get(timeout=10)  
                print(f"✅ Sent to Kafka: {patient_data}")
            except KafkaError as e:
                print(f"❌ Kafka Error: {e}")
                time.sleep(10)  # Retry delay
        else:
            print("⚠️ No patient data found. Waiting for new data...")

        time.sleep(5)  # Stream every 5 seconds

if __name__ == "__main__":
    try:
        produce_patient_data()
    finally:
        producer.close()
