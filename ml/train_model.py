import pandas as pd
import joblib
import os
from influxdb_client import InfluxDBClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

# InfluxDB Config
INFLUXDB_URL = "http://127.0.0.1:8086"
INFLUXDB_TOKEN = "AW_pSzi-vK51tPBoTLkUSOkG548UECZOO2NyUbQEUNCy6qyjPcm558P2OzKKBpzM9vfJ3gV9eiJloKKptjzhPg=="
INFLUXDB_ORG = "Data Engineering"
INFLUXDB_BUCKET = "patient-monitoring"

# Query to retrieve labeled patient data
QUERY = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["Patient_ID", "Heart_Rate", "Blood_Sugar_Level", "Temperature", "BMI", "Critical"])
'''

def query_data_from_influx():
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    query_api = client.query_api()
    result = query_api.query_data_frame(QUERY)
    client.close()

    # If result is a list of dataframes, concatenate
    if isinstance(result, list):
        if not result:
            raise Exception("❌ No data returned from InfluxDB!")
        result = pd.concat(result, ignore_index=True)

    if result.empty:
        raise Exception("❌ No data returned from InfluxDB!")

    return result


def preprocess(df: pd.DataFrame):
    df = df.dropna()

    # Define logic for critical (diabetic) condition
    def is_critical(row):
        return int(
            row['Blood_Sugar_Level'] > 180 or  # diabetes threshold
            row['Heart_Rate'] > 100 or
            row['Temperature'] > 38.5 or
            row['BMI'] > 30
        )

    df['critical'] = df.apply(is_critical, axis=1)

    X = df[['Blood_Sugar_Level', 'Heart_Rate', 'Temperature', 'BMI']]
    y = df['critical']

    return X, y

def train_and_save_model():
    print("📡 Fetching data from InfluxDB...")
    df = query_data_from_influx()

    X, y = preprocess(df)

    print(f"✅ Retrieved {len(df)} patient records.")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print("📊 Evaluation:")
    print(classification_report(y_test, y_pred))

    # 🔥 Save model in ../models directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_dir = os.path.abspath(os.path.join(base_dir, "..", "models"))
    os.makedirs(model_dir, exist_ok=True)

    model_path = os.path.join(model_dir, "patient_risk_model.pkl")
    joblib.dump(model, model_path)

    print(f"✅ Model saved at: {model_path}")

if __name__ == "__main__":
    train_and_save_model()
