from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'machine_raw',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='filter-group',         # własny group_id — niezależny od innych
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Progi bezpośrednich alertów na surowych odczytach
THRESHOLDS = {
    'temperature': {'warn': 75.0,  'critical': 88.0},  # °C
    'pressure':    {'warn': 2.5,   'critical': 1.5},   # bar — niskie = problem
    'vibration':   {'warn': 4.5,   'critical': 6.5},   # mm/s
}

print('Filtr surowych odczytów uruchomiony\n')

for message in consumer:
    r = message.value
    alerts = []

    if r['temperature'] > THRESHOLDS['temperature']['critical']:
        alerts.append(f"TEMP CRITICAL ({r['temperature']}°C)")
    elif r['temperature'] > THRESHOLDS['temperature']['warn']:
        alerts.append(f"TEMP WARN ({r['temperature']}°C)")

    if r['pressure'] < THRESHOLDS['pressure']['critical']:
        alerts.append(f"CIŚN CRITICAL ({r['pressure']}bar)")
    elif r['pressure'] < THRESHOLDS['pressure']['warn']:
        alerts.append(f"CIŚN WARN ({r['pressure']}bar)")

    if r['vibration'] > THRESHOLDS['vibration']['critical']:
        alerts.append(f"DRG CRITICAL ({r['vibration']}mm/s)")
    elif r['vibration'] > THRESHOLDS['vibration']['warn']:
        alerts.append(f"DRG WARN ({r['vibration']}mm/s)")

    if alerts:
        level = 'CRITICAL' if any('CRITICAL' in a for a in alerts) else 'WARN'
        print(f"[{level}] {r['machine_id']:12} | " + ' | '.join(alerts))
