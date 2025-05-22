from kafka import KafkaProducer
import json
import time
import random

# Inisialisasi producer Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'sensor-data'  # nama topic yang sama dengan consumer.py harus pakai ini juga

try:
    while True:
        # Buat data suhu dan kelembaban random
        data = {
            'suhu': round(random.uniform(20, 30), 2),
            'kelembaban': round(random.uniform(60, 80), 2)
        }

        # Kirim data ke Kafka topic
        producer.send(topic, value=data)
        print(f"Data terkirim: {data}")

        time.sleep(5)  # jeda 5 detik sebelum kirim data berikutnya

except KeyboardInterrupt:
    print("Producer dihentikan")
    producer.close()
