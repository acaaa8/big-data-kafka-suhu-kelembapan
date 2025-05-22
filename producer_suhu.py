import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    data = {
        "gudang_id": random.choice(gudangs),
        "suhu": random.randint(75, 90)
    }
    producer.send('sensor-suhu-gudang', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
