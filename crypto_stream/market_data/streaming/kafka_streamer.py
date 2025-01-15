# kafka_streamer.py
from ...kafka_utils.producer import send_to_kafka
from ...configs.config import load_config, get_stream_options
import urllib
import aiohttp
import json
import asyncio
from ..processing.data_process import process_quote_data
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def handle_message(exchange, data_type, symbol, msg):
    # Parse the original message data
    data = json.loads(msg.data)
    
    # Add data_type to the message payload
    enriched_data = {
        "type": data_type,  # Add the data_type field
        "exchange": exchange,  # Also include exchange for consistency
        "symbol": symbol,     # Include symbol for completeness
        **data               # Spread the original data
    }
    enriched_data['type'] = data_type
    
    # Create topic name specific to the exchange
    topic = f"crypto-ticks-{exchange}-{data_type}"
    
    # Send enriched data to Kafka
    send_to_kafka(
        topic=topic,
        key=f"{exchange}-{symbol}-{data_type}",  # Unique key for partitioning
        value=json.dumps(enriched_data)  # Serialize the enriched message
    )
    
    # Process enriched data locally if needed
    asyncio.create_task(process_quote_data(enriched_data))

class KafkaStreamer:
    def __init__(self):
        self._stream_options = get_stream_options()
        self._options = urllib.parse.quote_plus(json.dumps(self._stream_options))
        self._URL = f"ws://localhost:8001/ws-stream-normalized?options={self._options}"

    async def run(self):
        '''
        run the asynchronous function
        '''
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self._URL) as websocket:
                async for msg in websocket:
                    # Dynamically extract exchange and symbol from the message or stream options
                    try:
                        # Assuming the incoming message has the exchange and symbol in its data
                        data = json.loads(msg.data)
                        exchange = data.get("exchange")
                        symbol = data.get("symbol")
                        data_type = data['type']
                        if data_type == 'book_snapshot' and data['depth'] == 1:
                            data_type = 'quote'
                        elif data_type == 'book_snapshot':
                            level = data['depth']
                            data_type = f'book_snapshot_level_{level}'
                        if exchange and symbol:
                            # Handle the message for the corresponding exchange and symbol
                            await handle_message(exchange, data_type, symbol, msg)
                    except Exception as e:
                        print(f"Error processing message: {e}")

def main():
    """Entry point for the streamer"""
    streamer = KafkaStreamer()
    try:
        asyncio.run(streamer.run())
    except KeyboardInterrupt:
        logger.info("Streamer stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()