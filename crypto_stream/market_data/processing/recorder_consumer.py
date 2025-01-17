import asyncio
from asyncio import to_thread
import json
import logging
from pathlib import Path

from crypto_stream.kafka_utils.consumer import create_kafka_consumer
from crypto_stream.storage.redis.tick_cache import RedisTickCache
from crypto_stream.storage.disk.writer import DiskWriter
from crypto_stream.utils.data_utils import format_quote_data, calculate_quote_spreads, prepare_storage_quote_data
from crypto_stream.configs.config import get_kafka_options

logger = logging.getLogger(__name__)

class RecorderConsumer:
    def __init__(self, data_dir='/media/cong1989/Expansion1/work_for_autonomous/crypto_data'):
        self.data_dir = Path(data_dir)
        self.cache = RedisTickCache()
        self.writer = DiskWriter(self.data_dir)
        self.consumer = create_kafka_consumer()
        
    async def process_message(self, msg):
        """Process a single Kafka message"""
        try:
            raw_data = json.loads(msg.value().decode('utf-8'))
            tick_data = format_quote_data(raw_data)
            spreads = calculate_quote_spreads(tick_data)
            storage_data = prepare_storage_quote_data(tick_data, spreads)
            self.cache.add_tick(tick_data, storage_data)
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def run(self):
        """Main consumer loop"""
        try:
            # Subscribe to Kafka topic
            #kafka_config = get_kafka_config()
            # temporary topic
            kafka_topic = f"crypto-ticks-binance-futures-quote"
            self.consumer.subscribe([kafka_topic])
            
            # Start the flush loop
            flush_task = asyncio.create_task(self.writer.start_flush_loop(self.cache))
            logger.info("Started flush loop")
            
            while True:
                try:
                    msg = await to_thread(self.consumer.poll, 1.0)
                    if msg is None:
                        continue
                        
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                        
                    await self.process_message(msg)
                        
                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}", exc_info=True)
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Fatal error in consumer: {e}", exc_info=True)
        finally:
            self.writer.running = False
            self.consumer.close()
            
def main():
    """Entry point for the recorder consumer"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    print('start main')
    consumer = RecorderConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        logger.info("Recorder stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()
