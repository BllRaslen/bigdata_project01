# # KafkaMessageProducerWithLimit.py
#
# # Import necessary libraries and modules
# import pandas as pd
# from confluent_kafka import Producer
# import time
#
# # Read the CSV file into a Pandas DataFrame
# df = pd.read_csv('KaggleDataset.csv', encoding='ISO-8859-1')
#
# # Kafka producer configuration
# producer_config = {
#     'bootstrap.servers': 'localhost:9092',  # Kafka broker address
# }
#
# # Create a Kafka producer instance
# producer = Producer(producer_config)
#
# # Kafka topic to which you want to push messages
# kafka_topic = 'car_topic'  # Kafka topic name
#
# # The maximum duration for the producer to run (in seconds)
# # It takes data every 10 seconds, consider adjusting based on your requirements
# max_duration_seconds = 12
#
# # Record the start time
# start_time = time.time()
#
# # Run the producer for a maximum duration
# while time.time() - start_time < max_duration_seconds:
#     # Convert each row to JSON and push to Kafka
#     for _, row in df.iterrows():
#         message_value = row.to_json()
#         producer.produce(kafka_topic, value=message_value)
#
#     # Wait for any outstanding messages to be delivered and delivery reports to be received
#     producer.flush()
#
#     # Print a message indicating that messages have been sent
#     print(f"Data from Traffic.csv pushed to Kafka successfully.")
#
#     # Sleep for 1 second before sending the next batch of messages
#     time.sleep(1)
#


# Import necessary libraries and modules
import time
from kafka import KafkaProducer
import pandas as pd

# Set up Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'car_topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Load data from Traffic.csv

traffic_data = pd.read_csv('KaggleDataset.csv', encoding='ISO-8859-1')

# Run the producer in an infinite loop
iteration = 1
while True:
    print(f"Iteration {iteration}/Infinity: Data from Traffic.csv pushed to Kafka successfully.")

    # Send data to Kafka
    for _, data_row in traffic_data.iterrows():
        producer.send(topic, value=data_row.to_json().encode('utf-8'))

    # Pause for a while before the next iteration (adjust the sleep time as needed)
    time.sleep(20)

    # Increment iteration counter
    iteration += 1