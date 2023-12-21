from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

def create_spark_session():
    """
    Creates a Spark session with the necessary configurations.
    """
    return (
        SparkSession.builder.appName("KafkaStructuredStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def train_ml_model(static_data):
    """
    Trains a machine learning model using Logistic Regression classifier.
    """
    # Add a "label" column to the static data
    static_data_with_label = static_data.withColumn("label", col("Quantity").cast(IntegerType()))

    # Create a feature vector using VectorAssembler
    assembler = VectorAssembler(inputCols=["Year", "Month", "Quantity", "Pct"], outputCol="Cfeatures")
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    # Create a pipeline for the machine learning model
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(static_data_with_label)  # Use the static data with the label

    return model

def main():
    """
    Main function for the Spark application.
    """
    # Create a Spark session
    spark = create_spark_session()

    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "car_topic"

    # Define the schema for the data
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # Read static data from a CSV file
    static_data_path = "KaggleDataset.csv"
    static_data = spark.read.csv(static_data_path, header=True, schema=schema)

    # Read streaming data from Kafka
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Cast the value column from Kafka as STRING
    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    # Parse the JSON data and select fields
    parsed_stream_df = json_stream_df.select(from_json("value", schema).alias("data")).select("data.*")

    # Add a "label" column to the streaming data
    labeled_stream_df = parsed_stream_df.withColumn("label", col("Quantity").cast(IntegerType()))

    # Filter the streaming data based on a condition
    processed_stream_df = labeled_stream_df.filter("Quantity > 50")

    # Train or load the machine learning model
    ml_model = train_ml_model(static_data)

    # Use the machine learning model
    ml_result = ml_model.transform(processed_stream_df)

    # Write the results to the console in append mode
    query = (
        ml_result.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()

# Check whether the script is being run as the main program
if __name__ == "__main__":
    main()
