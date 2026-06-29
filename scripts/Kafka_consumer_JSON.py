# ***************************************************************************************
# Script Name: Recomart Kafka Consumer
# ***************************************************************************************
# Objective: To consume clickstream logs from kafka topic and stored in data lake as json file.
# ****************************************************************************************
# Import dependency library

import os
from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timedelta
from pathlib import Path

# 1. Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'recomart-consumer-group1',
    'auto.offset.reset': 'earliest'
}

# Define the local Windows path

script_dir = os.getcwd() 
base_path = os.path.join(script_dir, "raw_data")
source = "streaming"
data_type="clickstream_logs"


# 1. Get current time components
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%H%M%S")

#output_dir = r'C:\Users\avima\DM4ML\recomart\raw_data\streaming'
output_dir = os.path.join(base_path, source, data_type, year, month, day)


if not os.path.exists(output_dir):
    os.makedirs(output_dir)

consumer = Consumer(conf)
consumer.subscribe(['weblogs']) 

print(f"Started consuming. Saving files to: {output_dir}")

try:
    file_counter = 1
    # Create the first file
    current_file = open(os.path.join(output_dir, f"clickstreamdata_{file_counter}.json"), "a")
    line_count = 0

    while True:
        msg = consumer.poll(1.0) # Timeout of 1.0s

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Decode message and write to file
        data = msg.value().decode('utf-8')
        print(f"RECEIVED: {data}")
        current_file.write(data + "\n")
        # After writing a line, force it to disk:
        current_file.flush() 
        os.fsync(current_file.fileno()) # Extra safety for Windows
        
        line_count += 1

        # Rotate file every 1000 lines (optional logic to keep files manageable)
        if line_count >= 1000:
            current_file.close()
            file_counter += 1
            current_file = open(os.path.join(output_dir, f"clickstreamdata_{file_counter}.json"), "a")
            line_count = 0
            print(f"Rotated to file: data_{file_counter}.json")

except KeyboardInterrupt:
    pass
finally:
    current_file.close()
    consumer.close()