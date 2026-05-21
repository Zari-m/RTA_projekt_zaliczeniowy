from fastapi import FastAPI
from pydantic import BaseModel, Field
import pickle, numpy as np

app = FastAPI(title='Machine Anomaly Detection API')

model = pickle.load(open('machine_model.pkl', 'rb'))

FEATURES = ['temp_slope', 'pres_slope', 'vib_zscore']


class MachineFeatures(BaseModel):
    temp_slope:  float = Field(..., example=0.12,  description='Trend temperatury [°C/odczyt]')
    pres_slope:  float = Field(..., example=-0.02, description='Trend ciśnienia [bar/odczyt]')
    vib_zscore:  float = Field(..., example=0.8,   description='Skok drgań względem normy [σ]')


class PredictionResponse(BaseModel):
    failure_probability: float
    health_score:        int
    risk_level:          str
    is_anomaly:          bool


def risk_label(prob):
    if prob >= 0.75: return 'CRITICAL'
    if prob >= 0.50: return 'HIGH'
    if prob >= 0.25: return 'MEDIUM'
    return 'LOW'


@app.post('/score', response_model=PredictionResponse)
def score(data: MachineFeatures):
    X = np.array([[data.temp_slope, data.pres_slope, data.vib_zscore]])
    prob   = float(model.predict_proba(X)[0, 1])   # prawdop. klasy 1 (awaria)
    health = max(0, min(100, int((1.0 - prob) * 100)))
    return {
        'failure_probability': round(prob, 4),
        'health_score':        health,
        'risk_level':          risk_label(prob),
        'is_anomaly':          bool(prob >= 0.5),
    }


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/model-info')
def model_info():
    return {
        'type':     'RandomForestClassifier',
        'features': FEATURES,
        'n_estimators': model.n_estimators,
        'output': {
            'failure_probability': '0.0 – 1.0',
            'health_score':        '0 (awaria) – 100 (ideał)',
            'risk_level':          'LOW | MEDIUM | HIGH | CRITICAL',
        }
    }
