# services/processing_service.py

def is_valid_range(value, min_val, max_val):
    try:
        return min_val <= float(value) <= max_val
    except:
        return False

def parse_blood_pressure(bp):
    try:
        systolic, diastolic = map(int, bp.split('/'))
        return systolic, diastolic
    except:
        return None, None

def generate_alerts(patient):
    alerts = []

    if not is_valid_range(patient['Heart_Rate'], 60, 100):
        alerts.append(f"Abnormal heart rate: {patient['Heart_Rate']} bpm")

    if not is_valid_range(patient['Blood_Sugar_Level'], 70, 140):
        alerts.append(f"High blood sugar: {patient['Blood_Sugar_Level']} mg/dL")

    if not is_valid_range(patient['Temperature'], 36.1, 37.8):
        alerts.append(f"Abnormal temperature: {patient['Temperature']} °C")

    systolic, diastolic = parse_blood_pressure(patient.get('Blood_Pressure', '0/0'))
    if systolic and (systolic > 140 or diastolic > 90):
        alerts.append(f"High blood pressure: {systolic}/{diastolic} mmHg")

    if not is_valid_range(patient['BMI'], 18.5, 30):
        alerts.append(f"Abnormal BMI: {patient['BMI']}")

    if not is_valid_range(patient['Height'], 100, 210):  # cm
        alerts.append(f"Suspicious height: {patient['Height']} cm")

    if not is_valid_range(patient['Weight'], 40, 150):  # kg
        alerts.append(f"Suspicious weight: {patient['Weight']} kg")

    return alerts


def process_patient_data(patient):
    # Clean data (basic validation could be extended)
    cleaned_data = {
        key: patient.get(key, None) for key in patient
    }

    # Generate alerts
    alerts = generate_alerts(cleaned_data)

    # Build alert message if needed
    alert_message = {
        "Patient_ID": cleaned_data.get("Patient_ID"),
        "Alerts": alerts,
        "Timestamp": cleaned_data.get("Last_Visit_Date"),
    } if alerts else None

    # Dashboard format (simplified, can be expanded)
    dashboard_data = {
        "Patient_ID": cleaned_data.get("Patient_ID"),
        "Heart_Rate": cleaned_data.get("Heart_Rate"),
        "Temperature": cleaned_data.get("Temperature"),
        "Blood_Sugar_Level": cleaned_data.get("Blood_Sugar_Level"),
        "BMI": cleaned_data.get("BMI"),
        "Last_Visit": cleaned_data.get("Last_Visit_Date")
    }

    return cleaned_data, alert_message, dashboard_data
