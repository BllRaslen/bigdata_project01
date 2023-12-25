from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Spark oturumunu başlatın
spark = SparkSession.builder.appName('KafkaSparkIntegration').getOrCreate()

# JSON'u ayrıştırmak için şemayı tanımlayın
schema = StructType([
    StructField('Year', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('Make', StringType(), True),
    StructField('Model', StringType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('Pct', FloatType(), True),  # 'Pct' nin bir float olduğunu varsayalım
])

# Kafka'dan veri okuyun
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'car_topic') \
    .load()

# JSON verisini ayrıştırın
parsed_df = df.select(from_json(col('value').cast('string'), schema).alias('data')).select('data.*')

# 'parsed_df' üzerinde işleme veya makine öğrenimi görevlerinizi gerçekleştirin

# Gerekli Kafka paketleriyle Spark oturumunu başlatın
spark = SparkSession.builder \
    .appName('KafkaSparkIntegration') \
    .getOrCreate()

# Akış sorgusunu başlatın
query = parsed_df \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

# Akışın tamamlanmasını bekleyin
query.awaitTermination()
