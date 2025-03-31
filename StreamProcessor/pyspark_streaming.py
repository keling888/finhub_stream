from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType
from pyspark.sql.streaming import DataStreamWriter

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka2BQ") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .getOrCreate()

# Define Kafka parameters
KAFKA_BROKER = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
TOPIC = "finnhub_stocks"

# s: symbol p: price t: UNIX milliseconds timestamp v: volume 
# Define Schema for Incoming Data
schema = StructType() \
    .add("s", StringType()) \
    .add("p", FloatType()) \
    .add("t", LongType()) \
    .add("v", FloatType())

# Read Stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='A44QXPKZY4HLYPR2' password='RNWzjJ6twD+MZ5UzsKAvT9eIrwHd8VJ/SgB0vT/DPfKZ6t10lfWY1zuHaZpjDzdi';") \
    .load()

# Convert Kafka message from JSON
df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("s", "symbol") \
    .withColumnRenamed("p", "price") \
    .withColumnRenamed("t", "timestamp") \
    .withColumnRenamed("v", "volume")

# Write Stream to BigQuery
df.writeStream \
    .format("bigquery") \
    .option("table", "calcium-centaur-453322-v9.finnhub_stock.finnhub_stock") \
    .option("checkpointLocation", "gs://sparkstreaming_keling/checkpoints/") \
    .option("temporaryGcsBucket", "sparkstreaming_keling_temp")\
    .outputMode("append") \
    .start() \
    .awaitTermination()
