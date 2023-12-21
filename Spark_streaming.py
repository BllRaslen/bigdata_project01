#spark_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.appName('KafkaSparkIntegration').getOrCreate()

# Define the schema to parse JSON
schema = StructType([
    StructField('Year', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('Make', StringType(), True),
    StructField('Model', StringType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('Pct', FloatType(), True),  # Assuming 'Pct' is a float
])

# Read data from Kafka
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'car_topic') \
    .load()

# Parse JSON data
parsed_df = df.select(from_json(col('value').cast('string'), schema).alias('data')).select('data.*')

# Perform your processing or machine learning tasks on 'parsed_df'
# Initialize Spark session with necessary Kafka packages
spark = SparkSession.builder \
    .appName('KafkaSparkIntegration') \
    .getOrCreate()
# Start the streaming query
query = parsed_df \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

# Wait for the streaming to finish
query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when
# from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
#
# # Initialize Spark session
# spark = SparkSession.builder.appName('KafkaSparkIntegration').getOrCreate()
#
# # Define the schema to parse JSON
# schema = StructType([
#     StructField('Year', IntegerType(), True),
#     StructField('Month', IntegerType(), True),
#     StructField('Make', StringType(), True),
#     StructField('Model', StringType(), True),
#     StructField('Quantity', IntegerType(), True),
#     StructField('Pct', FloatType(), True),  # Assuming 'Pct' is a float
# ])
#
# # Read data from Kafka
# df = spark \
#     .readStream \
#     .format('kafka') \
#     .option('kafka.bootstrap.servers', 'localhost:9092') \
#     .option('subscribe', 'car_topic') \
#     .load()
#
# # Parse JSON data
# parsed_df = df.select(from_json(col('value').cast('string'), schema).alias('data')).select('data.*')
#
# # Add a new column 'QuantityAndKDV' based on the condition
# processed_df = parsed_df.withColumn(
#     'QuantityAndKDV',
#     when(col('Quantity') > 50, col('Quantity') - 5).otherwise(col('Quantity'))
# )
#
# # Perform your processing or machine learning tasks on 'processed_df'
# # You can add ML tasks here using the 'processed_df' DataFrame
#
# # Start the streaming query
# query = processed_df \
#     .writeStream \
#     .outputMode('append') \
#     .format('console') \
#     .start()
#
# # Wait for the streaming to finish
# query.awaitTermination()



