import os

# ml.py
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, expr
import shutil
import os

os.environ["SPARK_LOCAL_IP"] = "192.168.1.255"

def train_ml_model(static_data):
    # "label" column in static data
    static_data_with_label = static_data.withColumn("label", col("Quantity").cast(IntegerType()))

    # Create a feature vector using VectorAssembler
    assembler = VectorAssembler(inputCols=["Year", "Month", "Quantity", "Pct"], outputCol="Cfeatures")
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    # Create a pipeline for the machine learning model
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(static_data_with_label)  # Use the static data with the label

    if os.path.exists("/home/bll/PycharmProjects/bigdata_project/metaData"):
        shutil.rmtree("/home/bll/PycharmProjects/bigdata_project/metaData")

    model.save("/home/bll/PycharmProjects/bigdata_project/metaData")
    # Print the output of the machine learning model without truncation
    ml_result = model.transform(static_data_with_label)
    # ml_result.show(truncate=False, n=ml_result.count())

    return model

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

    # Verify PySpark installation
    try:
        from pyspark import SparkContext, SparkConf

        print("PySpark is installed and accessible.")
    except ImportError:
        print("PySpark is not installed or not accessible.")


    def create_spark_session():
        return (
            SparkSession.builder.appName("MLTraining")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.reverseProxy", "true")
            .config("spark.ui.reverseProxyUrl", "http://localhost:4049")
            .config("spark.ui.port", "4049")  # Set the SparkUI port explicitly
            .getOrCreate()
        )


    # Kafka configuration
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "car_topic"

    # Define the schema for the data
    # Define the schema for the data
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # Read static data from a CSV file
    static_data_path = "KaggleDataset.csv"
    static_data = create_spark_session().read.csv(static_data_path, header=True, schema=schema)

    # Train the machine learning model
    train_ml_model(static_data)
