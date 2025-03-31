# **Real-Time Financial Monitoring with BigQuery, Spark Streaming, Kafka, and Grafana**

## **Overview**
This project builds a real-time financial monitoring system using **Google BigQuery, Apache Kafka, Pyspark, and Grafana**. The system integrated real-time data from Finnhub.io and batch data from the Fed, processes stock market and interest rate data, visualizes it on **Grafana dashboards**, and sets up alerts for market conditions.

![Dashboard](images/dashboard.gif)

## **1️⃣ Data Pipeline Architecture**
### **🔹 Ingestion Layer**
- **Websocket from Finnhub.io :** Continuously poll data from API
- **Federal Reserve Bank**: csv files uploaded to Google Storage.
### **🔹 Broker Layer**
- **Framework**: Apache Kafka
- **Kafka Connector**: Confluent Cloud
### **🔹 Streaming Layer**
- **Framework**: Spark Structured Streaming
- **Deployment**: Google Cloud Dataproc
### **🔹 Database Layer**
- Spark Streaming writes records to BigQuery tables.
### **🔹 Visualization Layer**
- Grafana connects to BigQuery using Plugins
```
(1) Fetch WebSocket Data
 Local Machine / GCP Compute Engine VM
          ┌──────────────┐      ┌─────────────────┐
          │ WebSocket API│      │ CSV files from  │
          └──────┬───────┘      │ the FED         │
                 │              └────────┬────────┘
                 ▼                       │
       (2) Publish to Kafka (Confluent Cloud)    │
       ┌───────────────────┐                     │
       │  Kafka Producer   │                     │
       └──────┬────────────┘                     │
              │                                  │
              ▼                                  │
      (3) Kafka Topic: "finnhub-trades"          │
       ┌───────────────────┐                     │
       │   Kafka Broker    │                     │
       └──────┬────────────┘                     │
              │                                  │
              ▼                                  │
       (4) Read with Spark Streaming (Dataproc)  │
       ┌───────────────────┐                     │
       │   Spark Job on    │                     │
       │  GCP Dataproc VM  │                     │
       └──────┬────────────┘                     │
              │                                  │
              ▼                                  |
       (5) Store in BigQuery                     |
       ┌───────────────────┐                     |
       │  Google BigQuery  │◄────────────────────|
       └──────┬────────────┘                     
              │                                  
              ▼                                  
       (6) Visualization
       ┌──────────┐
       │ Grafana  │
       │ Dashboard│
       └──────────┘
```

## **2️⃣ Ingestion and Broker**
```python
import json
import websocket
from kafka import KafkaProducer

# Kafka Configuration 
KAFKA_BROKER = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
TOPIC = "finnhub_stocks"

# Configure Kafka Producer with Confluent Cloud
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="public key",
    sasl_plain_password="private key",
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,  # Serialize keys
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize values
)

def on_message(ws, message):
    """Receives WebSocket data and publishes to Kafka."""
    try:
        parsed_message = json.loads(message)
        trade_data = parsed_message.get("data", [])
        
        # Handle WebSocket "ping" messages to prevent disconnection
        if parsed_message.get("type") == "ping":
            print("Received ping from Finnhub, keeping connection alive.")
            return  
        
        for trade in trade_data:
            key = trade["s"]
            try:
                producer.send(TOPIC, key = key, value=trade)
            except Exception as e:
                print(f"having problems sending data to kafka:{e}") 
            print(f"Published to Kafka: {trade, key}")

    except json.JSONDecodeError as e:
        print(f"JSON Parsing Error: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    """Sends subscription messages when the WebSocket connection opens."""
    symbols = ["AAPL", "AMZN", "NVDA", "META", "BINANCE:BTCUSDT"]
    for symbol in symbols:
        message = json.dumps({"type": "subscribe", "symbol": symbol})
        ws.send(message)
        print(f"Sent subscription request: {message}") 
 
if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=cv7r4h1r01qpecih6vpgcv7r4h1r01qpecih6vq0",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()
```
## **3️⃣ Spark Streaming on Dataproc Clusters**

### **🔹 PySpark Kafka Consumer Code**

```python
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
# Define Schema
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
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='my public key' password='my private key';") \
    .load()

# Convert Kafka message from JSON
# I also did most of the transforming job using Pyspark like flattening nested json data
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
    # checkpoint
    .option("checkpointLocation", "gs://sparkstreaming_keling/checkpoints/") \
    .option("temporaryGcsBucket", "sparkstreaming_keling_temp")\
    # prevent dubble writing
    .outputMode("append") \
    .start() \
    # never stops -- what makes streaming possible
    .awaitTermination()

```

### **🔹 Deployment on Dataproc**

1. Spark Streaming requires additional dependencies --automatically downloded from Maven 
2. Bigquery Dependencies --Locally downloaded and transfered to Google Storage
3. Grant needed roles to this service account before submitting jobs: BigQuery Data Editor, Dataproc Editor, Dataproc Worker Editor, Service Account User, Storage Admin, Storage Object Viewer
```bash
gcloud dataproc jobs submit pyspark gs://sparkstreaming_keling/pyspark_streaming.py \
    --cluster=spark-cluster --region=us-central1 \
    --jars gs://sparkstreaming_keling/jars/spark-bigquery-with-dependencies_2.12-0.24.2.jar \
    --properties spark.jars.packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3"
```
## **4️⃣ Data Transforming in Bigquery**
I did most of the heavy lifting for data transformation in PySpark. In BigQuery, I dealt with data formatting issues that caused incompatibility with the Grafana platform.
```sql
CREATE OR REPLACE TABLE `calcium-centaur-453322-v9.finnhub_stock.finnhub_trade_transformed` 
AS
SELECT 
    TIMESTAMP(observation_date) AS time,  
    volume,
    price,
    symbol
FROM `calcium-centaur-453322-v9.finnhub_stock.finnhub_trade`;

CREATE OR REPLACE TABLE `calcium-centaur-453322-v9.finnhub_stock.interest_rate_transformed` 
AS
SELECT 
    TIMESTAMP(observation_date) AS time,  
    DFF 
FROM `calcium-centaur-453322-v9.finnhub_stock.interest_rate`;
```
## **5️⃣ Visualization in Grafana**
I chose Grafana for visualiztion for its ability to refesh dashboards in 500ms, while visualization tool in GCP requires at least 1 min.
### **🔹 Configurations**
I deployed Grafana on GCP VM:
```bash
sudo apt-get update
sudo apt-get install -y grafana
sudo systemctl enable grafana-server
sudo systemctl start grafana-server
sudo grafana-cli plugins install grafana-bigquery-datasource
sudo systemctl restart grafana-server
```
Grant BigQuery Job User to this service account user.
### **🔹 Connecting Bigquery to Grafana**
Set Up BigQuery as a Data Source in Grafana and use Google JWT File for authentication.

**✨ Hooray -- now that the `finnhub_stock` and `interest_rate` table are connected to Grafana**
### **🔹 Queries for Visualizations**
```sql
-- Tading Volume (cumulative) --
SELECT 
    time,
    symbol,
    SUM(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_volume
FROM `calcium-centaur-453322-v9.finnhub_stock.finnhub_stock`
WHERE symbol = 'AAPL';

-- Trading Price --
SELECT price, symbol, time 
FROM `calcium-centaur-453322-v9.finnhub_stock.finnhub_stock` 
WHERE symbol = 'AAPL' 
```
## **6️⃣ Challenges**
Initially I wanted to build a dashboard that is truly real-time -- updating every 500ms. However, since my database is built in BigQuery and it usually takes 2-3 seconds at minimum for a query to get cold started in BigQuery, which is the bottleneck of achieving streaming in visualization. In the end, I was able to make the dashboard update every 5 seconds at best by building it on Grafana. To really improve this situation, I plan to migrate the database to InfluxDB where nearly real-time updates are said to be available.




