import re
import nltk
import json
import subprocess
from confluent_kafka import Consumer

# Replace with your Kafka bootstrap server and topic names
bootstrap_servers = "localhost:9092"
topic_names = ["news_topic"]

# Create a Kafka consumer
kafka_consumer = Consumer({'bootstrap.servers': bootstrap_servers, 'group.id': "my_group"})

# Subscribe to the topics
kafka_consumer.subscribe(topic_names)

# HDFS output directory
hdfs_output_dir = "/BigData/hive"

# Open CSV file and create CSV writers for each topic
while True:
    msg = kafka_consumer.poll(timeout=1000)

    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    topic = msg.topic()

    # Decode the message value from bytes to string
    message_value = msg.value().decode()

    try:
        # Deserialize the JSON string into a list of objects
        decoded_message = json.loads(message_value)
        print(decoded_message)

        cleaned_keyword = re.sub(r'\s+', ' ', decoded_message['keyword'].replace('\n', ' ')).strip()
        decoded_message['keyword'] = cleaned_keyword

        # Write the decoded_message data to HDFS
        hdfs_output_file = f"{hdfs_output_dir}/{topic}/{msg.offset()}.json"
        with subprocess.Popen(["hdfs", "dfs", "-put", "-", hdfs_output_file], stdin=subprocess.PIPE) as proc:
            proc.stdin.write(json.dumps(decoded_message).encode())
            proc.stdin.close()
            

    except json.JSONDecodeError:
        print(f"Failed to decode message: {message_value}")
