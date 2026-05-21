from kafka import KafkaConsumer, KafkaProducer
import json, requests

consumer = KafkaConsumer(
    'machine_aggregated',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='ml-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = 'http://localhost:8001/score'
ALERT_PROB_THRESHOLD = 0.50

print('Konsument ML uruchomiony — czyta z machine_aggregated\n')
print(f"{'Maszyna':<12} {'Health':>6} {'P(awaria)':>10} {'Risk'}")
print('─' * 42)

for message in consumer:
    agg = message.value

    if agg['n_readings'] < 10:
        continue

    payload = {
        'temp_slope':  agg['temp_slope'],
        'pres_slope':  agg['pres_slope'],
        'vib_zscore':  agg['vib_zscore'],
    }

    try:
        resp   = requests.post(API_URL, json=payload, timeout=2)
        result = resp.json()
    except requests.RequestException as e:
        print(f'[!] API niedostępne: {e}')
        continue

    prob   = result['failure_probability']
    health = result['health_score']
    risk   = result['risk_level']

    if result['is_anomaly'] or prob >= ALERT_PROB_THRESHOLD:
        alert = {
            **agg,
            'failure_probability': prob,
            'health_score':        health,
            'risk_level':          risk,
            'alert_source':        'ml_model',
        }
        alert_producer.send('alerts', value=alert)
        alert_producer.flush()

    # Kolorystyka w terminalu wg poziomu ryzyka
    if risk == 'CRITICAL':
        line = f'>>> AWARIA   P={prob:.0%}'
    elif risk == 'HIGH':
        line = f'!   WYSOKIE  P={prob:.0%}'
    elif risk == 'MEDIUM':
        line = f'~   UWAGA    P={prob:.0%}'
    else:
        line = '    OK'

    print(f'{agg["machine_id"]:<12} {health:>6}  {prob:>9.1%}  {risk:<9}  {line}')
