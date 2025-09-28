import requests
import time
import json
import os
from kafka import KafkaProducer

API_KEY = os.getenv("NEWSAPI_KEY")
if not API_KEY:
    raise ValueError("NEWSAPI_KEY environment variable not set")
TOPIC = "news-raw"
BOOTSTRAP = os.getenv("KAFKA_URL", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_news(topic="technology", page_size=5):
    url = f"https://newsapi.org/v2/everything?q={topic}&pageSize={page_size}&sortBy=publishedAt&apiKey={API_KEY}"
    r = requests.get(url)
    return [{"title": a["title"]} for a in r.json().get("articles", []) if a.get("title")]


while True:
    news = fetch_news("technology", 5)
    for article in news:
        producer.send(TOPIC, article)
    print(f"Sent {len(news)} headlines")
    time.sleep(30)
