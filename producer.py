
from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Pięć maszyn w hali produkcyjnej
MACHINES = ['PRASA', 'KOMPRESOR', 'TOKARKA', 'POMPA', 'SILNIK']

# Każda maszyna ma swoje bazowe parametry pracy — lekko różne
random.seed(7)
BASE = {
    m: {
        'temp':     random.uniform(55.0, 68.0),   # °C
        'pressure': random.uniform(4.2,  6.8),    # bar
        'vib':      random.uniform(0.8,  1.9),    # mm/s
    }
    for m in MACHINES
}
random.seed()

# Stan degradacji per maszyna: czy trwa, i który to krok
degradation = {m: {'active': False, 'step': 0} for m in MACHINES}


def generate_reading(machine_id):
    base = BASE[machine_id]
    d = degradation[machine_id]

    # Z prawdopodobieństwem 0.4% startuje degradacja (jeśli żadna nie trwa)
    if not d['active'] and random.random() < 0.004:
        d['active'] = True
        d['step'] = 0
        print(f'\n>>> START DEGRADACJI: {machine_id}\n')

    if d['active']:
        step = d['step']
        # Każdy krok pogarsza parametry maszyny
        temp     = base['temp']     + step * 1.4  + random.gauss(0, 0.5)
        pressure = base['pressure'] - step * 0.11 + random.gauss(0, 0.08)
        vib      = base['vib']      + step * 0.14 + random.gauss(0, 0.07)
        d['step'] += 1
        if d['step'] > 40:
            d['active'] = False
            print(f'\n>>> KONIEC DEGRADACJI: {machine_id} (symulowana naprawa)\n')
    else:
        # Normalna praca: wartości oscylują wokół bazy z małym szumem
        temp     = base['temp']     + random.gauss(0, 1.2)
        pressure = base['pressure'] + random.gauss(0, 0.15)
        vib      = base['vib']      + random.gauss(0, 0.12)

    return {
        'reading_id':  f'R{random.randint(10000,99999)}',
        'machine_id':  machine_id,
        'temperature': round(temp, 2),           # °C
        'pressure':    round(max(0.1, pressure), 3),  # bar
        'vibration':   round(max(0.0, vib), 3),  # mm/s
        'timestamp':   datetime.now().isoformat(),
    }

print('Producent uruchomiony — 5 maszyn, 1 odczyt/s per maszyna\n')
print(f"{'Maszyna':<12} {'Temp':>7} {'Ciśn':>7} {'Drg':>7}  Status")
print('─' * 52)

while True:
    for m in MACHINES:
        reading = generate_reading(m)
        producer.send('machine_raw', value=reading)

        status = 'DEGRADACJA !!!' if degradation[m]['active'] else 'OK'
        print(
            f"{m:<12}"
            f"{reading['temperature']:>6.1f}°C "
            f"{reading['pressure']:>6.2f}bar "
            f"{reading['vibration']:>6.3f}mm/s  {status}"
        )

    producer.flush()
    time.sleep(1.0)
