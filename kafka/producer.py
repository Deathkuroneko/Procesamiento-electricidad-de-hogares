# Kafka Producer - Simulación de streaming
# Lee datos energéticos y los envía a Kafka

import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_file = '../data/processed/household_power_consumption_cleaned.csv'

with open(csv_file, newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        producer.send('energy-consumption', row)
        print("Enviado:", row)
        time.sleep(1)  # Simula streaming (1 evento por segundo)

producer.flush()
producer.close()
print("Todos los datos han sido enviados a Kafka.")