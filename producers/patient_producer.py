import json
from kafka import KafkaProducer
from pymongo import MongoClient
from pymongo.errors import PyMongoError
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

def stream_new_patients():
    """Stream only newly inserted patients using MongoDB change streams."""
    pipeline = [{'$match': {'operationType': 'insert'}}]

    try:
        print(f"🚀 Watching for new patients in MongoDB to stream to Kafka topic: {RAW_DATA_TOPIC}")
        with patient_collection.watch(pipeline) as change_stream:
            for change in change_stream:
                new_patient = change['fullDocument']
                new_patient['_id'] = str(new_patient['_id'])  # Convert ObjectId to string
                try:
                    producer.send(RAW_DATA_TOPIC, new_patient)
                    print(f"✅ Sent to Kafka: {new_patient}")
                except KafkaError as ke:
                    print(f"❌ Kafka error: {ke}")
    except PyMongoError as pe:
        print(f"❌ MongoDB Change Stream error: {pe}")

if __name__ == "__main__":
    try:
        stream_new_patients()
    finally:
        producer.close()
