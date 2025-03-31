import json
import websocket
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = "Your message broker"
TOPIC = "finnhub_stocks"

# Configure Kafka Producer with Confluent Cloud
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="Your Username",
    sasl_plain_password="Your Password",
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
        "wss://ws.finnhub.io?token=$Your Finhub API token" ",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()
