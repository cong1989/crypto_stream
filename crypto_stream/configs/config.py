# config.py
import yaml
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "config.yaml"
KAFKA_BROKER = 'localhost:9092'

def load_config():
    with open(CONFIG_FILE, 'r') as file:
        return yaml.safe_load(file)

def get_stream_options():
    """Get the stream options from the configuration"""
    config = load_config()
    return config.get('stream_options', [])

def get_redis_options():
    config = load_config()
    return config.get('redis_options', [])

def get_disk_writer_options():
    config = load_config()
    return config.get('disk_writer_options', [])

def get_kafka_options():
    config = load_config()
    return config.get('kafka_options', [])

def get_recording_options():
    config = load_config()
    return config.get('recording_options', [])

def get_sampled_data_manager_options():
    config = load_config()
    return config.get('sampled_data_manager_options', [])