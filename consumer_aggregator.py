from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque
import statistics
import json

consumer = KafkaConsumer(
    'machine_raw',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='aggregator-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

agg_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

WINDOW = 15   # rozmiar okna kroczącego

def empty_window():
    # Osobne kolejki dla każdego sensora
    return {
        'temperature': deque(maxlen=WINDOW),
        'pressure':    deque(maxlen=WINDOW),
        'vibration':   deque(maxlen=WINDOW),
    }

# Okna kroczące per maszyna
windows = defaultdict(empty_window)

def compute_slope(values):
    """
    Trend: porównuje średnią drugiej połowy okna z pierwszą.
    Wynik dodatni = wartość rośnie. Wynik ujemny = wartość spada.
    Zwraca zmianę na jeden odczyt.
    """
    if len(values) < WINDOW:
        return 0.0
    lst = list(values)
    mid = len(lst) // 2
    return (statistics.mean(lst[mid:]) - statistics.mean(lst[:mid])) / mid


def compute_zscore(values, current):
    """
    O ile odchyleń standardowych bieżący odczyt różni się od
    średniej okna. Wysoki z-score = nagły skok względem własnej normy.
    """
    if len(values) < 5:
        return 0.0
    mu = statistics.mean(values)
    try:
        sigma = statistics.stdev(values)
    except statistics.StatisticsError:
        return 0.0
    return 0.0 if sigma < 0.001 else (current - mu) / sigma


print(f'Agregator uruchomiony — okno {WINDOW} odczytów per maszyna')
print(f"{'Maszyna':<12} {'n':>3} {'temp_slope':>11} {'pres_slope':>11} {'vib_z':>7}")
print('─' * 52)

for message in consumer:
    r = message.value
    mid = r['machine_id']
    w = windows[mid]

    # Dołącz bieżący odczyt do okien kroczących
    w['temperature'].append(r['temperature'])
    w['pressure'].append(r['pressure'])
    w['vibration'].append(r['vibration'])

    n = len(w['temperature'])

    # Oblicz cechy trendowe
    temp_slope  = compute_slope(w['temperature'])
    pres_slope  = compute_slope(w['pressure'])    # ujemny = ciśnienie spada
    vib_zscore  = compute_zscore(w['vibration'], r['vibration'])

    aggregated = {
        'machine_id':    mid,
        'timestamp':     r['timestamp'],
        'n_readings':    n,
        # Cechy trendowe — wejście dla reguł i modelu ML
        'temp_slope':    round(temp_slope, 4),
        'pres_slope':    round(pres_slope, 4),
        'vib_zscore':    round(vib_zscore, 4),
        # Średnie z okna — do wyświetlania w alertach
        'temp_avg':      round(statistics.mean(w['temperature']), 2),
        'pressure_avg':  round(statistics.mean(w['pressure']), 3),
        'vib_avg':       round(statistics.mean(w['vibration']), 3),
        # Bieżące wartości surowe
        'current_temp':     r['temperature'],
        'current_pressure': r['pressure'],
        'current_vib':      r['vibration'],
    }

    agg_producer.send('machine_aggregated', value=aggregated)

    print(
        f"{mid:<12} {n:>3} "
        f"{temp_slope:>+10.3f} "
        f"{pres_slope:>+10.3f} "
        f"{vib_zscore:>+6.2f}σ"
    )

agg_producer.flush()
