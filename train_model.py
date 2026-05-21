import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import pickle

np.random.seed(42)

N_NORMAL = 3000
N_FAIL   = 200   # ~6% — tyle degradacji pojawi się w strumieniu

# Normalna praca: trendy bliskie zera, brak skoków
normal = pd.DataFrame({
    'temp_slope':  np.random.normal(0.0,  0.28, N_NORMAL).clip(-0.9, 0.9),
    'pres_slope':  np.random.normal(0.0,  0.03, N_NORMAL).clip(-0.1, 0.1),
    'vib_zscore':  np.random.normal(0.0,  0.7,  N_NORMAL).clip(-2.2, 2.2),
    'label': 0
})

# Degradacja: temperatura rośnie, ciśnienie spada, drgania skaczą
failing = pd.DataFrame({
    'temp_slope':  np.random.normal(2.1,  0.55, N_FAIL).clip(1.1, 5.0),
    'pres_slope':  np.random.normal(-0.14, 0.04, N_FAIL).clip(-0.4, -0.05),
    'vib_zscore':  np.random.normal(3.2,  0.8,  N_FAIL).clip(1.5, 6.5),
    'label': 1
})

df = pd.concat([normal, failing], ignore_index=True).sample(frac=1, random_state=42)
FEATURES = ['temp_slope', 'pres_slope', 'vib_zscore']
X, y = df[FEATURES], df['label']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)
y_pred = clf.predict(X_test)

print('=' * 55)
print('RANDOM FOREST — wyniki na zbiorze testowym')
print('=' * 55)
print(classification_report(y_test, y_pred, target_names=['normalna', 'awaria']))

print('Ważność cech:')
for feat, imp in sorted(zip(FEATURES, clf.feature_importances_), key=lambda x: -x[1]):
    bar = '█' * int(imp * 40)
    print(f'  {feat:<14} {bar} {imp:.3f}')

with open('machine_model.pkl', 'wb') as f:
    pickle.dump(clf, f)

print('\nModel zapisany: machine_model.pkl')
