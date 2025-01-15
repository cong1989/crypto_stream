import redis
import json
import pandas as pd
from crypto_stream.configs.config import get_redis_options

class RedisTickCache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db)
        self.cache_key_prefix = 'crypto_ticks:'
        
    def get_cache_key(self, exchange, data_type, symbol, date_hour):
        return f"{self.cache_key_prefix}{exchange}:{data_type}:{symbol}:{date_hour}"
        
    def add_tick(self, tick_data, storage_data):
        # Extract components for the key
        exchange = tick_data['market_data']['exchange']
        data_type = tick_data['market_data']['type']
        symbol = tick_data['market_data']['symbol']
        timestamp = pd.Timestamp(tick_data['timestamps']['event_time'])
        date_hour = timestamp.strftime('%Y-%m-%d:%H')
        # Create cache key
        cache_key = self.get_cache_key(exchange, data_type, symbol, date_hour)
        # Add to Redis list and set expiry
        self.redis.rpush(cache_key, json.dumps(storage_data))
        self.redis.expire(cache_key, get_redis_options()['redis_tick_cache']['redis_expiry'])
        
    def get_keys_to_flush(self):
        """Get all keys that need to be flushed to disk"""
        return self.redis.keys(f"{self.cache_key_prefix}*")
        
    def get_and_clear_ticks(self, cache_key):
        """Get all ticks for a key and remove them from Redis"""
        pipe = self.redis.pipeline()
        pipe.lrange(cache_key, 0, -1)  # Get all items
        pipe.delete(cache_key)         # Delete the key
        results = pipe.execute()
        return [json.loads(tick) for tick in results[0]]
