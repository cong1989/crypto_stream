from confluent_kafka import Consumer
from crypto_stream.configs.config import KAFKA_BROKER

def create_kafka_consumer():
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'crypto-consumer-group',
        'auto.offset.reset': 'earliest',
    })