import json
import logging
import time

import pandas as pd
import redis

from crypto_stream.configs.config import get_redis_options


class RedisTickCache:
    def __init__(self, topic, host="localhost", port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db)
        self.cache_key_prefix = f"crypto_ticks:"
        self.topic = topic
        self.out_of_order_count = 0
        # Create logger for this class
        self.logger = logging.getLogger("RedisTickCache")

    def get_cache_key(self, exchange, data_type, symbol, date_hour):
        return f"{self.cache_key_prefix}{exchange}:{data_type}:{symbol}:{date_hour}"

    def get_keys_to_flush(self, exchange, data_type):
        """Get all keys that need to be flushed to disk, sorted by time"""
        # Get all matching keys
        keys = self.redis.keys(f"{self.cache_key_prefix}*")
        # First decode bytes to strings
        keys = [k.decode() if isinstance(k, bytes) else k for k in keys]

        # Then filter by exchange and data_type
        keys = [k for k in keys if f":{exchange}:" in k and f":{data_type}:" in k]

        # Sort keys based on their timestamp component
        def get_key_time(key):
            # Key format is crypto_ticks:exchange:type:symbol:YYYY-MM-DD:HH
            try:
                time_part = key.split(":")[-2:]  # Get ['YYYY-MM-DD', 'HH']
                return f"{time_part[0]}:{time_part[1]}"  # Combine to 'YYYY-MM-DD:HH'
            except IndexError:
                return "0"  # Default value for invalid keys

        # Sort keys by their timestamps
        sorted_keys = sorted(keys, key=get_key_time)

        self.logger.debug(
            f"Found {len(sorted_keys)} keys to flush, first few timestamps:"
        )
        for key in sorted_keys[:3]:
            self.logger.debug(f"  {get_key_time(key)}")

        return sorted_keys

    def add_tick(self, tick_data, storage_data):
        # Extract components for the key
        exchange = tick_data["market_data"]["exchange"]
        data_type = tick_data["market_data"]["type"]
        symbol = tick_data["market_data"]["symbol"]
        timestamp = pd.Timestamp(tick_data["timestamps"]["event_time"])
        date_hour = timestamp.strftime("%Y-%m-%d:%H")
        # Create cache key
        cache_key = self.get_cache_key(exchange, data_type, symbol, date_hour)
        # Debug: Check existing ticks
        existing = self.redis.lrange(cache_key, 0, 5)  # Get first 5 ticks
        if existing:
            self.logger.debug(f"Last 5 ticks for {symbol} before new tick {timestamp}:")
            for tick in existing:
                tick_data = json.loads(tick)
                self.logger.debug(f"Tick time: {tick_data['timestamp']}")

        # Check order for monitoring
        latest_tick = self.redis.lindex(cache_key, -1)
        if latest_tick:
            latest_tick = json.loads(latest_tick)
            latest_time = pd.Timestamp(latest_tick["timestamp"])

            if timestamp < latest_time:
                self.out_of_order_count += 1
                self.logger.warning(
                    f"Out of order tick detected:\n"
                    f"Symbol: {symbol}\n"
                    f"New tick time: {timestamp}\n"
                    f"Latest tick time: {latest_time}\n"
                    f"Total out of order count: {self.out_of_order_count}"
                )

        # Add to Redis list and set expiry
        self.redis.rpush(cache_key, json.dumps(storage_data))
        self.redis.expire(
            cache_key, get_redis_options()["redis_tick_cache"]["redis_expiry"]
        )

    # def get_and_clear_ticks(self, cache_key):
    #    """Get all ticks for a key and remove them from Redis"""
    #    pipe = self.redis.pipeline()
    #    pipe.lrange(cache_key, 0, -1)  # Get all items
    #    pipe.delete(cache_key)         # Delete the key
    #    results = pipe.execute()
    #    return [json.loads(tick) for tick in results[0]]

    def get_and_clear_ticks(self, cache_key):
        """Get all ticks atomically using rename"""
        temp_key = f"temp:{cache_key}:tmp{int(time.time() * 1000)}"

        # Atomically rename the key to temp key
        if not self.redis.rename(cache_key, temp_key):
            return []  # Key doesn't exist

        # Now we can take our time reading the temp key
        try:
            ticks = self.redis.lrange(temp_key, 0, -1)
            return [json.loads(tick) for tick in ticks]
        finally:
            # Clean up temp key
            self.redis.delete(temp_key)
