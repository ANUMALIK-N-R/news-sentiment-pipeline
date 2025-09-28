import streamlit as st
import pandas as pd
import altair as alt
from kafka import KafkaConsumer
import json
import time
import os

BOOTSTRAP = os.getenv("KAFKA_URL", "kafka:9092")
TOPIC = "news-predictions"

st.title("Real-Time News Sentiment Dashboard")

# Initialize consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    consumer_timeout_ms=1000  # stop iteration if no message
)

data = []
placeholder = st.empty()

while True:
    # Poll for new messages
    for msg in consumer:
        article = msg.value
        data.append(article)

    # Create dataframe
    df = pd.DataFrame(data)
    with placeholder.container():
        st.write(df.tail(5))
        if not df.empty:
            chart_data = df.groupby(
                "sentiment").size().reset_index(name="count")
            chart = alt.Chart(chart_data).mark_bar().encode(
                x="sentiment", y="count", color="sentiment"
            )
            st.altair_chart(chart, use_container_width=True)

    time.sleep(2)
