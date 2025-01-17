#!/bin/bash

# Check for required environment variable
if [ -z "$TARDIS_API_KEY" ]; then
    echo "Error: TARDIS_API_KEY environment variable not set"
    echo "Please set it with: export TARDIS_API_KEY=your_key"
    exit 1
fi

# Install package
pip install -e .

# Start services using Docker Compose
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Start data collection
crypto-stream start