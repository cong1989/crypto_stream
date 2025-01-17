import os
import asyncio
from asyncio import to_thread
import json
import logging
from pathlib import Path
from datetime import datetime
from crypto_stream.storage.redis.tick_cache import RedisTickCache
from crypto_stream.storage.disk.writer import DiskWriter
from crypto_stream.utils.data_utils import format_quote_data, calculate_quote_spreads, prepare_storage_quote_sampling_data
from crypto_stream.configs.config import get_kafka_options, get_recording_options 
from crypto_stream.kafka_utils.consumer import create_kafka_consumer
from .samplers.precise_sampler import SampledDataManager, EnhancedRedisTickCache
logger = logging.getLogger(__name__)

class SamplingQuoteRecorderConsumer:
    def __init__(self, data_dir, topic):
        self._data_dir = Path(data_dir)
        self._consumer = create_kafka_consumer()
        #self._kafka_topics = ['crypto-ticks-binance-futures-quote', 'crypto-ticks-binance-quote', 'crypto-ticks-bitmex-quote']
        #self._kafka_topics = [ 'crypto-ticks-binance-quote']
        self._kafka_topics = [topic]
        self._consumer.subscribe(self._kafka_topics)
        self._cache = EnhancedRedisTickCache(topic)
        self._writer = DiskWriter(self._data_dir, topic)
        
    async def process_message(self, msg):
        """Process a single Kafka message"""
        try:
            raw_data = json.loads(msg.value().decode('utf-8'))
            tick_data = format_quote_data(raw_data)
            spreads = calculate_quote_spreads(tick_data)
            storage_data = prepare_storage_quote_sampling_data(tick_data, spreads)
            self._cache.add_tick(tick_data, storage_data)
        except Exception as e:
            print('sampling_recorder_consumer process_message error:', e)
            #logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def run(self):
        """Main consumer loop"""
        try:
            # Subscribe to Kafka topic
            #kafka_config = get_kafka_config()
            # temporary topic
            # Start the flush loop

            # there is a race condition between this flush thing and sampling function
            flush_task = asyncio.create_task(self._writer.start_flush_loop(self._cache))
            logger.info("Started flush loop")
            
            while True:
                try:
                    msg = await to_thread(self._consumer.poll, 0.001)
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
            self._writer.running = False
            self._consumer.close()
            
def run_sampling(topic):
    """Entry point for the recorder consumer"""

    consumer = SamplingQuoteRecorderConsumer(data_dir = get_recording_options()['recorder_consumer_dir'], topic = topic)
    log_dir = os.path.join(get_recording_options()['recorder_consumer_dir'],'logs', consumer._kafka_topics[0])
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file with timestamp
    log_file = os.path.join(log_dir,  f'sampling_recorder_consumer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,

        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),  # Write to file
            #logging.StreamHandler()         # Also write to console
        ]
    )
    
    print('start main')
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        logger.info("Recorder stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

def main():
    from multiprocessing import Pool
    topics = ['crypto-ticks-binance-futures-quote', 'crypto-ticks-binance-quote', 'crypto-ticks-bitmex-quote']
#    topics = ['crypto-ticks-binance-futures-quote']
    with Pool(3) as p:
        p.map(run_sampling, topics)

if __name__ == "__main__":
    main()
