from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

def create_spark_session():
    """
    Gerekli konfigürasyonlarla bir Spark oturumu oluşturur.
    """
    return (
        SparkSession.builder.appName("KafkaStructuredStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def train_ml_model(static_data):
    """
    Logistic Regression sınıflandırıcısını kullanarak bir makine öğrenimi modeli eğitir.
    """
    # Statik veriye "label" sütunu ekleyin
    static_data_with_label = static_data.withColumn("label", col("Quantity").cast(IntegerType()))

    # VectorAssembler kullanarak özellik vektörü oluşturun
    assembler = VectorAssembler(inputCols=["Year", "Month", "Quantity", "Pct"], outputCol="Cfeatures")
    lr = LogisticRegression(featuresCol="Cfeatures", labelCol="label")

    # Makine öğrenimi modeli için bir pipeline oluşturun
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(static_data_with_label)  # Etiketli statik veriyi kullanın

    return model

def main():
    """
    Spark uygulamasının ana fonksiyonu.
    """
    # Spark oturumu oluşturun
    spark = create_spark_session()

    # Kafka konfigürasyonu
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "car_topic"

    # Veri için şema tanımlayın
    schema = StructType().add("Year", IntegerType()).add("Month", IntegerType()) \
        .add("Make", StringType()).add("Model", StringType()) \
        .add("Quantity", IntegerType()).add("Pct", FloatType())

    # CSV dosyasından statik veriyi okuyun
    static_data_path = "KaggleDataset.csv"
    static_data = spark.read.csv(static_data_path, header=True, schema=schema)

    # Kafka'dan akış verisi okuyun
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Kafka değer sütununu STRING olarak dönüştürün
    json_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) as value")

    # JSON verisini ayrıştırın ve alanları seçin
    parsed_stream_df = json_stream_df.select(from_json("value", schema).alias("data")).select("data.*")

    # Akış verisine "label" sütunu ekleyin
    labeled_stream_df = parsed_stream_df.withColumn("label", col("Quantity").cast(IntegerType()))

    # Akış verisini bir koşula göre filtreleyin
    processed_stream_df = labeled_stream_df.filter("Quantity > 50")

    # Makine öğrenimi modelini eğitin veya yükleyin
    ml_model = train_ml_model(static_data)

    # Makine öğrenimi modelini kullanın
    ml_result = ml_model.transform(processed_stream_df)

    # Sonuçları append modunda konsola yazın
    query = (
        ml_result.writeStream.outputMode("append")
        .format("console")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()

# Betik ana program olarak çalıştırılıyor mu kontrol edin
if __name__ == "__main__":
    main()
