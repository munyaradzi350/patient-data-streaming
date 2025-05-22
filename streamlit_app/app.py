import streamlit as st
import sys
import os

# Go up one level to import from ml/
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
ML_DIR = os.path.join(ROOT_DIR, 'ml')
sys.path.append(ML_DIR)

from predict import predict_risk as predict_patient_risk

# Set up Streamlit page
st.set_page_config(page_title="🧠 Patient Risk Classifier", layout="centered")

# Styling
st.markdown("""
    <style>
    .main {
        background-color: #f4f6f9;
        padding: 2rem;
        border-radius: 10px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.1);
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 24px;
        border-radius: 6px;
        font-size: 16px;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    .stNumberInput>div>input {
        background-color: #ffffff;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown("<h1 style='text-align: center;'>🧠 Real-Time Patient Risk Classifier</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center;'>Enter patient vitals to predict if they are in a <strong>critical</strong> condition.</p>", unsafe_allow_html=True)

# Input columns
col1, col2, col3, col4 = st.columns(4)

with col1:
    blood_sugar = st.number_input("🩸 Blood Sugar (mg/dL)", min_value=50.0, max_value=500.0, value=110.0, format="%.1f")

with col2:
    heart_rate = st.number_input("❤️ Heart Rate (bpm)", min_value=30, max_value=200, value=80)

with col3:
    temperature = st.number_input("🌡️ Temperature (°C)", min_value=30.0, max_value=45.0, value=36.5, format="%.1f")

with col4:
    bmi = st.number_input("📊 BMI", min_value=10.0, max_value=50.0, value=22.5, format="%.1f")

if st.button("🧪 Predict"):
    try:
        result = predict_patient_risk(blood_sugar, heart_rate, temperature, bmi)
        st.markdown("---")
        st.subheader("🔍 Prediction Result:")

        if result["Critical"] == 1:
            st.error(f"🚨 Patient is **CRITICAL**!\n\nConfidence: **{result['Probability']*100:.2f}%**")
        else:
            st.success(f"✅ Patient is **STABLE**.\n\nConfidence: **{result['Probability']*100:.2f}%**")
    except Exception as e:
        st.error(f"❌ Error during prediction: {str(e)}")
