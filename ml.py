import os
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType


def train_ml_model(static_data):
    # Cast "Quantity" column to IntegerType and use it as the label
    static_data_with_label = static_data.withColumn("label", col("Quantity").cast(IntegerType()))

    # Create a feature vector using VectorAssembler
    assembler = VectorAssembler(inputCols=["Year", "Month", "Quantity"], outputCol="features")

    # Logistic Regression model
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    # Create a pipeline for the machine learning model
    pipeline = Pipeline(stages=[assembler, lr])

    # Train and save the machine learning model
    model = pipeline.fit(static_data_with_label)
    model.save("/home/bll/PycharmProjects/bigdata_project/LRM/metadata")

    # Transform the data with the trained model
    ml_result = model.transform(static_data_with_label)

    # Show the ML result without truncation
    ml_result.show(truncate=False, n=ml_result.count())

    return model


if __name__ == "__main__":
    def create_spark_session():
        return (
            SparkSession.builder.appName("MLTraining")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.driver.host", "localhost")  # Set the driver host to suppress the hostname warning
            .config("spark.ui.reverseProxy", "true")  # Enable reverse proxy for Spark UI
            .config("spark.ui.reverseProxyUrl", "http://localhost:4040")  # Set the reverse proxy URL
            .getOrCreate()
        )


    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "car_topic"

    # Define the schema for the data
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # Read static data from a CSV file
    static_data_path = "KaggleDataset.csv"  # Adjust the path to your dataset
    static_data = create_spark_session().read.csv(static_data_path, header=True, schema=schema)

    # Train the machine learning model
    train_ml_model(static_data)
