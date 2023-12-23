from confluent_kafka import Consumer as KafkaConsumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

class KafkaStreamReader:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': "console-consumer-15074",
            'auto.offset.reset': 'earliest',
        }
        self.consumer = KafkaConsumer(self.consumer_config)
        self.topic = topic

    def subscribe_to_topic(self):
        self.consumer.subscribe([self.topic])

    def read_stream(self, spark, schema):
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

            # Parse the received message value and append it to the DataFrame
            json_data = msg.value().decode('utf-8')
            data = spark.read.json(spark.sparkContext.parallelize([json_data]), schema=schema)
            streaming_df = streaming_df.union(data)

            # Your further processing logic goes here...

            # For example, you can write the streaming DataFrame to another Kafka topic
            # streaming_df.selectExpr("to_json(struct(*)) as value").write \
            #     .format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
            #     .option("topic", "processed_traffic_data_topic").save()

            # Or you can write it to a file or any other sink...

            # Print the received message value
            print('Received message: {}'.format(json_data))

def main():
    bootstrap_servers = 'localhost:9092'
    group_id = 'console-consumer-15074'
    kafka_topic = 'car_topic'

    # Create Spark session
    spark = SparkSession.builder.appName("KafkaStructuredStreaming").getOrCreate()

    # Define the schema for parsing the JSON messages
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # Create KafkaStreamReader instance
    kafka_reader = KafkaStreamReader(bootstrap_servers, group_id, kafka_topic)

    # Subscribe to the Kafka topic
    kafka_reader.subscribe_to_topic()

    # Read streaming data from Kafka
    kafka_reader.read_stream(spark, schema)

if __name__ == "__main__":
    main()
