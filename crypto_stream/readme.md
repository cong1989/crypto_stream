# Crypto Market Data Collector

A package for collecting and sampling cryptocurrency market data using Tardis and Kafka.

## Prerequisites

- Docker
- Java 17(for Kafka)   conda install -c conda-forge openjdk=17
- Python 3.8+
- Kafka installation in `/opt/kafka`
- redis-server

# build kafka
wget https://downloads.apache.org/kafka/
tar -xvzf kafka.tgz
sudo mv kafka-src /opt/kafka
cd /opt/kafka
./gradlew jar -PscalaVersion


pip install tardis-dev
sudo apt update
sudo apt install redis-server

1. Clone the repository:
```bash
git clone <your-repo-url>
cd crypto_stream
```

2. Install the package:
```bash
pip install -e .
```

## Setup and Running

### 1. Start Tardis Machine

```bash
# Create cache directory
mkdir -p ./host-cache-dir

# Run Tardis Machine
docker run -v ./host-cache-dir:/.cache \
    -p 8000:8000 -p 8001:8001 \
    -e "TM_API_KEY=your_tardis_key" \
    -d tardisdev/tardis-machine
```

### 2. Start Kafka Services

```bash
# Start Zookeeper
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# In another terminal, start Kafka
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

### 3. Run Data Collection

```bash
# Start the streamer
python -m crypto_stream.market_data.streaming.kafka_streamer

# Start the sampler (in another terminal)
python -m crypto_stream.market_data.processing.samplers.precise_sampler
```

## Service Management Scripts

Create these helper scripts in your project:

### start_services.sh
```bash
#!/bin/bash

# Start Tardis Machine
docker run -v ./host-cache-dir:/.cache \
    -p 8000:8000 -p 8001:8001 \
    -e "TM_API_KEY=$TARDIS_API_KEY" \
    -d tardisdev/tardis-machine

# Wait for Tardis to start
sleep 5

# Start Zookeeper
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

# Wait for Zookeeper to start
sleep 5

# Start Kafka
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

# Wait for Kafka to start
sleep 5

echo "All services started"
```

### stop_services.sh
```bash
#!/bin/bash

# Stop Kafka
/opt/kafka/bin/kafka-server-stop.sh

# Stop Zookeeper
/opt/kafka/bin/zookeeper-server-stop.sh

# Stop Tardis Machine
docker stop $(docker ps -q --filter ancestor=tardisdev/tardis-machine)

echo "All services stopped"
```

Make the scripts executable:
```bash
chmod +x start_services.sh stop_services.sh
```

## Configuration

Key configuration files:
- `configs/config.yaml`: Main configuration file
- `/opt/kafka/config/server.properties`: Kafka configuration
- `TM_API_KEY` environment variable for Tardis Machine

## Logging

Logs are stored in:
- `logs/streamer.log`: Streamer logs
- `logs/sampler.log`: Sampler logs
- Kafka logs: `/opt/kafka/logs`

## Data Storage

- Raw ticks: `<data_dir>/raw`
- Sampled data: `<data_dir>/sampled`
- Redis cache: Temporary storage for processing

## Monitoring

Check service status:
```bash
# Check Tardis Machine
docker ps | grep tardis-machine

# Check Kafka topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Check Redis
redis-cli info
```

## Troubleshooting

Common issues and solutions:

1. Tardis Machine not starting:
```bash
docker logs $(docker ps -q --filter ancestor=tardisdev/tardis-machine)
```

2. Kafka connection issues:
```bash
# Check if Kafka is running
ps aux | grep kafka
# Check Kafka logs
tail -f /opt/kafka/logs/server.log
```

3. Redis issues:
```bash
redis-cli ping  # Should return PONG
```