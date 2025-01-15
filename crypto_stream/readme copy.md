# Crypto 

A market data processing system with Kafka integration.

## Installation

```bash
pip install -e .
```

## Prerequisites

### Tardis Machine Setup

Run Tardis Machine using Docker:
```bash
docker run -v ./host-cache-dir:/.cache -p 8000:8000 -p 8001:8001 \
    -e "TM_API_KEY=TD.jaUmdpEuPS8hRuM5.aopHDk1HO1OFYj3.3-v7THR-FwfjxMP.Ik1HBaGbHbvz8rH.iqgDLyH2Uu4nkgD.ibSh" \
    -d tardisdev/tardis-machine
```

Set Tardis Machine environment variables:
```bash
export TM_API_KEY="tardis_key"
export TM_PORT=8000
export TM_CACHE_DIR="/path/to/cache"
export TM_CLUSTER_MODE=true
export TM_DEBUG=true
export TM_CLEAR_CACHE=false
```

### Kafka Setup

1. Install Java (required for Kafka)

2. Install and Configure Kafka:
```bash
# Extract Kafka
tar -xzf kafka_2.13-3.9.0.tgz
mv kafka_2.13-3.9.0 /opt/kafka

# Start Kafka services
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# Verify Kafka is running
bin/kafka-topics.sh --list --zookeeper localhost:2181
```

3. Configure Kafka for External Access:

Edit `/opt/kafka/config/server.properties`:
```properties
listeners=INTERNAL://localhost:9092
advertised.listeners=INTERNAL://<external-ip>:9092
listener.security.protocol=PLAINTEXT
```

Open firewall port:
```bash
sudo ufw allow 9092
```

4. Configure Zookeeper:

Add to Zookeeper configuration:
```properties
clientPortAddress=<external-ip>
```

### Example Kafka Consumer Configuration

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-tardis',
    bootstrap_servers='<external-ip>:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
```

## Usage

Run the market data streamer:
```bash
run-market-streamer
```

## Development

This project uses setuptools for packaging.