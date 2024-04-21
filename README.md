# Real-Time Data Processing with Kafka and Spark

This repository offers examples and a demonstration of real-time data processing using Apache Kafka and Apache Spark. It covers:

- Setting up a Kafka producer to dispatch data to a Kafka topic.
- Employing Spark Structured Streaming to ingest data from Kafka.
- Training a machine learning model using static data with Spark ML.
- Integrating machine learning model inference into Spark Structured Streaming.
- Writing the processed data back to Kafka or any other designated destination.

## Requirements

- Python 3.x
- Apache Kafka
- Apache Spark
- Confluent Kafka Python client
- PySpark
- pandas
- Kafka Python client
- A dataset (e.g., KaggleDataset.csv)

## Installation

1. Clone this repository:

git clone https://github.com/your-username/real-time-data-processing.git


2. Install dependencies:

pip install -r requirements.txt


3. Set up Apache Kafka and Apache Spark on your system. Refer to the official documentation for installation instructions.

4. Place your dataset (e.g., KaggleDataset.csv) in the repository root directory.

## Usage

- Start Apache Kafka and Apache Spark services.
- Run the Kafka producer to dispatch data to the Kafka topic:

python kafka_producer.py


- Execute the Spark Structured Streaming application to ingest data from Kafka, process it, and perform machine learning model inference:

python spark_ml_streaming.py


- View the processed data on the console or any other designated sink.

## Contributors

Bilal RASLEN

## License

This project is licensed under the MIT License - see the LICENSE file for details.
