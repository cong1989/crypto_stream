from confluent_kafka import Producer

from crypto_stream.configs.config import KAFKA_BROKER


def create_kafka_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER})


producer = create_kafka_producer()


# Function to send data to Kafka
def send_to_kafka(topic, key, value):
    try:
        producer.produce(topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
