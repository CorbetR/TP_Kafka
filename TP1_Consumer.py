# consumer.py

from kafka import KafkaConsumer
import json

# Configuration
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'tp-meteo'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume and display messages from Kafka
print("Consumer Kafka prêt à recevoir les données...")
for message in consumer:
    data = message.value
    print(f"Données météo reçues pour {data['city']}: {data}")
