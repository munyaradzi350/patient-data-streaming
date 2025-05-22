import joblib
import pandas as pd
import os

_model = None

def load_model():
    global _model
    if _model is not None:
        return _model

    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.abspath(os.path.join(base_dir, "..", "models", "patient_risk_model.pkl"))

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"❌ Model not found at {model_path}")

    _model = joblib.load(model_path)
    print(f"✅ Loaded model from {model_path}")
    return _model

def predict_risk(blood_sugar: float, heart_rate: float, temperature: float, bmi: float):
    model = load_model()

    input_df = pd.DataFrame([{
        "Blood_Sugar_Level": blood_sugar,
        "Heart_Rate": heart_rate,
        "Temperature": temperature,
        "BMI": bmi
    }])

    prediction = model.predict(input_df)[0]

    # Check if the model supports predict_proba and has 2 classes
    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(input_df)
        if probs.shape[1] == 2:
            probability = probs[0][1]  # Probability of class 1
        else:
            probability = probs[0][0]  # Fallback (if only 1 class)
    else:
        probability = 1.0 if prediction == 1 else 0.0

    return {
        "Blood_Sugar": blood_sugar,
        "Heart_Rate": heart_rate,
        "Temperature": temperature,
        "BMI": bmi,
        "Critical": int(prediction),
        "Probability": round(probability, 4)
    }

if __name__ == "__main__":
    result = predict_risk(blood_sugar=220.0, heart_rate=105, temperature=38.9, bmi=29.5)
    print("🩺 Prediction Result:")
    print(result)
