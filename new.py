from confluent_kafka import Consumer as KafkaConsumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

class KafkaStreamReader:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        }
        self.consumer = KafkaConsumer(self.consumer_config)
        self.topic = topic

    def subscribe_to_topic(self):
        # Belirtilen Kafka konusuna abone olun
        self.consumer.subscribe([self.topic])

    def read_stream(self, spark, schema):
        # Boş bir DataFrame oluşturun
        streaming_df = spark.createDataFrame([], schema=schema)

        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF():
                    continue
                else:
                    print(msg.error())
                    break

            # Alınan mesaj değerini ayrıştırın ve DataFrame'e ekleyin
            json_data = msg.value().decode('utf-8')
            data = spark.read.json(spark.sparkContext.parallelize([json_data]), schema=schema)
            streaming_df = streaming_df.union(data)

            print('Alınan mesaj: {}'.format(json_data))

def main():
    bootstrap_servers = 'localhost:9092'
    group_id = 'console-consumer-15074'
    kafka_topic = 'car_topic'

    # Spark oturumu oluşturun
    spark = SparkSession.builder.appName("KafkaStructuredStreaming").getOrCreate()

    # JSON mesajlarını ayrıştırmak için şemayı tanımlayın
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # KafkaStreamReader örneğini oluşturun
    kafka_reader = KafkaStreamReader(bootstrap_servers, group_id, kafka_topic)

    # Kafka konusuna abone olun
    kafka_reader.subscribe_to_topic()

    # Kafka'dan akış verisi okuyun
    kafka_reader.read_stream(spark, schema)

if __name__ == "__main__":
    main()
