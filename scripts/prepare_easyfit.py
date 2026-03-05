import csv
import re

input_file  = 'simulation_data_1.csv'
output_file = 'easyfit_data.csv'

def extract_producer_ts(message_content):
    """
    Extrae el timestamp UNIX del productor desde message_content.
    Formato: "Message from thread 5, 1771099205.378..."
    Ese número es el momento exacto en que el generador publicó el mensaje.
    """
    match = re.search(r'(\d{10,}\.\d+)', message_content)
    if match:
        return float(match.group(1))
    return None

rows = []
with open(input_file, 'r', encoding='utf-8') as fin:
    reader = csv.DictReader(fin)
    for row in reader:
        ts = extract_producer_ts(row['message_content'])
        if ts is not None:
            rows.append({
                'producer_ts': ts,
                'processing_time': float(row['processing_time'])
            })

# Ordenamos por timestamp del productor (orden real de publicación)
rows.sort(key=lambda r: r['producer_ts'])

# Calculamos tiempo entre llegadas y guardamos
with open(output_file, 'w', newline='', encoding='utf-8') as fout:
    writer = csv.DictWriter(fout, fieldnames=['inter_arrival_time', 'processing_time'])
    writer.writeheader()

    for i in range(1, len(rows)):
        iat = rows[i]['producer_ts'] - rows[i-1]['producer_ts']
        if iat >= 0:
            writer.writerow({
                'inter_arrival_time': round(iat, 6),
                'processing_time': rows[i]['processing_time']
            })

total = len(rows) - 1
iats = [rows[i]['producer_ts'] - rows[i-1]['producer_ts'] for i in range(1, len(rows)) if rows[i]['producer_ts'] - rows[i-1]['producer_ts'] > 0]
if iats:
    mean_iat = sum(iats) / len(iats)
    print(f"Listo: '{output_file}' — {len(iats)} registros generados (de {total} pares).")
    print(f"Timestamps extraídos del PRODUCTOR.")
    print(f"Media inter-arrival: {mean_iat:.6f} s  →  λ ≈ {1/mean_iat:.2f} msg/s")
    mean_proc = sum(r['processing_time'] for r in rows) / len(rows)
    print(f"Media processing:    {mean_proc:.6f} s  →  μ ≈ {1/mean_proc:.2f} msg/s")

