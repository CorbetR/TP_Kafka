# producer.py

import requests
import json
import time
from kafka import KafkaProducer

# Configuration
API_KEY = '5cd0c68e3773c0513b41138ade44163b'  # Replace with your actual API key
CITIES = ['Paris', 'London', 'Tokyo']
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'tp-meteo'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

# Sending weather data to Kafka in a loop
while True:
    for city in CITIES:
        data = get_weather_data(city)
        if data:
            weather_info = {
                'city': city,
                'temp': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity']
            }
            producer.send(KAFKA_TOPIC, value=weather_info)
            print(f"Données météo envoyées pour {city} dans le topic Kafka.")
    time.sleep(60)  # Wait for 60 seconds before fetching data again

