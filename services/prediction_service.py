from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Literal
import sys
import os

# Go two levels up to reach the realmedflow-kafka root
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
ML_DIR = os.path.join(ROOT_DIR, 'ml')
sys.path.append(ML_DIR)

from predict import predict_risk as predict_patient_risk

app = FastAPI(
    title="Patient Risk Prediction Service",
    description="API for predicting patient risk using ML model",
    version="1.0.0"
)

class PatientVitals(BaseModel):
    Heart_Rate: float = Field(..., example=102)
    Temperature: float = Field(..., example=38.5)
    BMI: float = Field(..., example=28.7)
    Blood_Sugar: float = Field(..., example=175)

class PredictionResult(BaseModel):
    Heart_Rate: float
    Temperature: float
    BMI: float
    Blood_Sugar: float
    Critical: Literal[0, 1]
    Probability: float

@app.get("/", tags=["Health Check"])
def root():
    return {"message": "🚀 Patient Prediction API is running!"}

@app.post("/predict", response_model=PredictionResult, tags=["Prediction"])
def get_prediction(vitals: PatientVitals):
    try:
        result = predict_risk(
            heart_rate=vitals.Heart_Rate,
            temperature=vitals.Temperature,
            bmi=vitals.BMI,
            blood_sugar=vitals.Blood_Sugar
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
