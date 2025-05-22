
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='sensor-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Menunggu data dari producer suhu dan kelembaban...")

for message in consumer:
    data = message.value
    print(f"Data diterima: {data}")
