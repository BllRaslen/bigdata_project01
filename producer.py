# Gerekli kütüphane ve modülleri içe aktarın
import time
from kafka import KafkaProducer
import pandas as pd

# Kafka üreticisini kurun
bootstrap_servers = 'localhost:9092'
topic = 'car_topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Trafik verisini yükle
traffic_data = pd.read_csv('KaggleDataset.csv', encoding='ISO-8859-1')

# Üreticiyi sonsuz bir döngüde çalıştırın
iteration = 1
while True:
    print(f"Iterasyon {iteration}/Sonsuz: KaggleDataset.csv'den Kafka'ya başarıyla veri gönderildi.")

    # Veriyi Kafka'ya gönder
    for _, data_row in traffic_data.iterrows():
        producer.send(topic, value=data_row.to_json().encode('utf-8'))

    # Bir sonraki iterasyon öncesi bir süre bekleyin (gerektiğinde uyku süresini ayarlayın)
    time.sleep(20)

    # İterasyon sayacını artırın
    iteration += 1
