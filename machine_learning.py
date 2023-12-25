import os
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

def train_ml_model(static_data):
    # "Quantity" sütununu IntegerType'a dönüştürerek etiket olarak kullanın
    static_data_with_label = static_data.withColumn("label", col("Quantity").cast(IntegerType()))

    # VectorAssembler kullanarak özellik vektörü oluşturun
    assembler = VectorAssembler(inputCols=["Year", "Month", "Quantity"], outputCol="features")

    # Lojistik Regresyon modeli
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    # Makine öğrenimi modeli için bir pipeline oluşturun
    pipeline = Pipeline(stages=[assembler, lr])

    # Makine öğrenimi modelini eğitin ve kaydedin
    model = pipeline.fit(static_data_with_label)
    model.save("/home/bll/PycharmProjects/bigdata_project/LRM/metadata")

    # Eğitilmiş modelle veriyi dönüştürün
    ml_result = model.transform(static_data_with_label)

    # Kesilme olmadan ML sonucunu gösterin
    ml_result.show(truncate=False, n=ml_result.count())

    return model

if __name__ == "__main__":
    def create_spark_session():
        return (
            SparkSession.builder.appName("MLTraining")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.driver.host", "localhost")  # Sürücü ana bilgisayarını, hostname uyarısını bastırmak için ayarlayın
            .config("spark.ui.reverseProxy", "true")  # Spark UI için ters proxy'yi etkinleştirin
            .config("spark.ui.reverseProxyUrl", "http://localhost:4040")  # Ters proxy URL'sini ayarlayın
            .getOrCreate()
        )

    # Kafka yapılandırması
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "car_topic"

    # Veri için şema tanımlayın
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # CSV dosyasından statik veriyi okuyun
    sta
