from confluent_kafka import Consumer, KafkaError

# Kafka tüketici yapılandırması
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka başlangıç sunucuları
    'group.id': 'console-consumer-15074',  # Tüketici grubu ID
    'auto.offset.reset': 'earliest',  # Konfigürasyon, Kafka'da başlangıç ​​offset'i olmadığında veya mevcut offset sunucuda artık bulunmuyorsa ne yapılacağını belirler.
}

consumer = Consumer(consumer_config)

kafka_topic = 'car_topic'  
consumer.subscribe([kafka_topic])

# Mesajları almak için döngü
while True:
    msg = consumer.poll(1.0)  # Poll işlemi için zaman aşımı (saniye cinsinden).

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # Bölüm olayının sonu
            continue
        else:
            print(msg.error())
            break

    # Alınan mesaj değerini yazdır
    # Kafka mesajı payload'unun ham ikili verisini Unicode bir dizeye çevir
    print('Alınan mesaj: {}'.format(msg.value().decode('utf-8')))
