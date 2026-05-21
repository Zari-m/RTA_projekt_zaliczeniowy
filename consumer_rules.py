from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'machine_aggregated',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='rules-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ALERT_THRESHOLD = 4   # minimalna liczba punktów żeby wysłać alert

def score_machine(agg):
    score = 0
    rules = []

    # R1: temperatura wyraźnie rośnie (trend)
    ts = agg['temp_slope']
    if ts > 1.2:
        score += 3
        rules.append(f'R1:temp_rosnie({ts:+.2f}°C/odczyt)')
    elif ts > 0.6:
        score += 1
        rules.append(f'R1b:temp_lekko_rosnie({ts:+.2f}°C/odczyt)')

    # R2: ciśnienie wyraźnie spada (trend ujemny)
    ps = agg['pres_slope']
    if ps < -0.08:
        score += 3
        rules.append(f'R2:cisn_spada({ps:+.3f}bar/odczyt)')
    elif ps < -0.04:
        score += 1
        rules.append(f'R2b:cisn_lekko_spada({ps:+.3f}bar/odczyt)')

    # R3: nagły skok drgań ponad normę maszyny
    vz = agg['vib_zscore']
    if vz > 2.5:
        score += 3
        rules.append(f'R3:drg_skok({vz:.2f}σ)')
    elif vz > 1.8:
        score += 1
        rules.append(f'R3b:drg_podwyzszone({vz:.2f}σ)')

    # R4: temperatura bezwzględnie wysoka (bez względu na trend)
    if agg['temp_avg'] > 87.0:
        score += 2
        rules.append(f"R4:temp_wysoka({agg['temp_avg']:.1f}°C)")

    return score, rules


print(f'Konsument regułowy uruchomiony (próg alertu: {ALERT_THRESHOLD} pkt)\n')

for message in consumer:
    agg = message.value

    # Ignoruj pierwsze odczyty — okno nie jest jeszcze wypełnione
    if agg['n_readings'] < 10:
        continue

    score, rules = score_machine(agg)

    if score >= ALERT_THRESHOLD:
        alert = {
            **agg,
            'score':        score,
            'rules':        rules,
            'alert_source': 'rules',
        }
        alert_producer.send('alerts', value=alert)
        alert_producer.flush()
        print(
            f"[ALERT {score:2d}pkt] {agg['machine_id']:12} | "
            + ' | '.join(rules)
        )
    else:
        print(f"[OK    {score:2d}pkt] {agg['machine_id']:12}")
