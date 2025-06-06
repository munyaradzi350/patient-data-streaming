import logging
from datetime import datetime

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def is_age_risk(age: int) -> bool:
    """
    Age-based mortality risk.
    - Children (<= 12 years)
    - Older adults (>= 65 years)
    """
    return age <= 12 or age >= 65


def check_mortality_risk(patient: dict) -> dict | None:
    """
    Strict mortality risk checker.
    An alert is triggered ONLY if ALL conditions are met:
    - Age: ≤12 or ≥65
    - Heart rate: ≥70
    - Systolic ≥120 AND Diastolic ≥80
    - Cholesterol not 'normal'
    - Genomic Info contains 'risk', 'variant', or 'mutated'
    """
    patient_id = patient.get("Patient_ID")
    age = int(patient.get("Age", 0))
    heart_rate = int(patient.get("Heart_Rate", 0))
    bp = patient.get("Blood_Pressure", "0/0")
    cholesterol = patient.get("Cholesterol_Level", "").lower()
    genomic_info = patient.get("Genomic_Info", "").lower()

    try:
        systolic, diastolic = map(int, bp.split("/"))
    except Exception as e:
        logging.warning(f"⚠️ Invalid Blood_Pressure format for Patient_ID {patient_id}: {e}")
        systolic, diastolic = 0, 0

    # Condition flags
    age_risk = is_age_risk(age)
    heart_risk = heart_rate >= 70
    bp_risk = systolic >= 120 and diastolic >= 80
    cholesterol_risk = cholesterol not in ["normal", ""]
    genomic_risk = any(term in genomic_info for term in ["risk", "variant", "mutated"])

    if all([age_risk, heart_risk, bp_risk, cholesterol_risk, genomic_risk]):
        logging.info(f"🚨 Mortality risk triggered for Patient_ID: {patient_id}")
        return {
            "Patient_ID": patient_id,
            "alert_type": "mortality_risk",
            "age": age,
            "heart_rate": heart_rate,
            "systolic": systolic,
            "diastolic": diastolic,
            "cholesterol_flag": int(cholesterol_risk),
            "genomic_flag": int(genomic_risk),
            "timestamp": datetime.utcnow().isoformat()
        }

    return None


def check_readmission_risk(patient: dict) -> dict | None:
    """
    Readmission risk checker:
    - Readmission within 45 days
    - Triggered if: medical history, comorbidities, or low physical activity
    """
    patient_id = patient.get("Patient_ID")
    diagnosis = patient.get("Diagnosis", "").lower()
    history = patient.get("Previous_Medical_History", "").strip().lower()
    comorbid = patient.get("Comorbidities", "").strip().lower()
    activity = patient.get("Physical_Activity", "").lower()
    last_visit_str = patient.get("Last_Visit_Date", None)

    days_since_last = None
    if last_visit_str:
        try:
            visit_date = datetime.strptime(last_visit_str, "%Y-%m-%d")
            days_since_last = (datetime.utcnow().date() - visit_date.date()).days
        except Exception as e:
            logging.warning(f"⚠️ Invalid Last_Visit_Date for Patient_ID {patient_id}: {e}")

    if (
        days_since_last is not None and days_since_last <= 45 and (
            "chronic" in diagnosis or
            history or
            comorbid or
            activity in ["low", "sedentary", "inactive"]
        )
    ):
        logging.info(f"🔁 Readmission risk triggered (within 45 days) for Patient_ID: {patient_id}")
        return {
            "Patient_ID": patient_id,
            "alert_type": "readmission_risk",
            "timestamp": datetime.utcnow().isoformat()
        }

    return None
