# ***************************************************************************************
# Script Name: Recomart Kafka Producer
# ***************************************************************************************
# Objective: To generates a clickstream logs every second and pushes it to Kafka Topics.
# ****************************************************************************************
# Install dependency library
#!pip install confluent-kafka pandas

import json
import uuid
import random
import time
from datetime import datetime
from datetime import timedelta
from confluent_kafka import Producer

# 1. Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'clickstream-producer'
}

topic_name = 'weblogs'
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent: {msg.key().decode('utf-8')} to {msg.topic()}")

# --- Data Generation & Production ---
actions = ['view', 'click', 'add_to_cart', 'purchase', 'No Action']
# ------------------------------------------------------------------------------------
def generate_transaction():
    
    return {
        "log_id": str(uuid.uuid4()),
        "user_id": f"USER_{random.randint(1, 500):03d}",
        "product_id": f"rm_{random.randint(1, 200):03d}",
        'action': random.choice(actions),
        #'timestamp': (datetime.now() - timedelta(minutes=random.randint(0, 43200))).isoformat(),
        'timestamp': (datetime.now()).isoformat(),
        'device': random.choice(['mobile', 'desktop', 'tablet'])
    }
# ------------------------------------------------------------------------------------
def start_streaming(topic_name, limit=500):
    print(f"Starting stream to topic: {topic_name}...")
    
    for i in range(limit):
        # Generate data
        data = generate_transaction()
        
        # Serialize to JSON
        message_key = data['log_id']
        message_value = json.dumps(data).encode('utf-8')
        
        # Produce to Kafka
        producer.produce(
            topic=topic_name,
            key=message_key,
            value=message_value,
            callback=delivery_report
        )
        
        # Serve delivery callbacks
        producer.poll(0)
        
        # Wait a moment to simulate real-time flow
        time.sleep(1)

    # Ensure all messages are sent
    producer.flush()
    print("Streaming job complete.")

if __name__ == "__main__":
    TARGET_TOPIC = 'weblogs'
    # Generate 100 messages
    start_streaming(TARGET_TOPIC, limit=30)
    