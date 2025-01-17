import json

import pandas as pd
import redis

from crypto_stream.configs.config import get_redis_options


class RedisTickCache:
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db)
        self.cache_key_prefix = "crypto_ticks:"
        self.last_processed_index_prefix = "last_processed:"  # Added for tracking

    def get_cache_key(self, exchange, data_type, symbol, date_hour):
        return f"{self.cache_key_prefix}{exchange}:{data_type}:{symbol}:{date_hour}"

    def get_last_processed_key(self, cache_key):
        """Get the key that stores the last processed index"""
        return f"{self.last_processed_index_prefix}{cache_key}"

    def add_tick(self, tick_data, storage_data):
        # Extract components for the key
        exchange = tick_data["market_data"]["exchange"]
        data_type = tick_data["market_data"]["type"]
        symbol = tick_data["market_data"]["symbol"]
        timestamp = pd.Timestamp(tick_data["timestamps"]["event_time"])
        date_hour = timestamp.strftime("%Y-%m-%d:%H")

        # Create cache key
        cache_key = self.get_cache_key(exchange, data_type, symbol, date_hour)
        # print('tick cache key:', cache_key)
        # print(json.dumps(storage_data))
        # print('------------------')

        # Add to Redis list and set expiry
        self.redis.rpush(cache_key, json.dumps(storage_data))
        self.redis.expire(
            cache_key, get_redis_options()["redis_tick_cache"]["redis_expiry"]
        )

    def get_keys_to_flush(self):
        """Get all keys that need to be flushed to disk"""
        return self.redis.keys(f"{self.cache_key_prefix}*")

    def get_and_clear_ticks(self, cache_key):
        """
        Get unprocessed ticks and clear older ones with no overlap
        Returns: new ticks to process (from last_processed + 1 to current)
        """
        try:
            # Get the last processed index
            last_idx_key = self.get_last_processed_key(cache_key)
            last_processed = int(self.redis.get(last_idx_key) or "-1")

            # Get current list length
            list_length = self.redis.llen(cache_key)
            if list_length == 0:
                return []

            pipe = self.redis.pipeline()

            # Get new items (no overlap because we start from last_processed + 1)
            pipe.lrange(cache_key, last_processed + 1, -1)

            # Keep a safety buffer
            safety_margin = get_redis_options()["redis_tick_cache"].get(
                "safety_margin", 1000
            )
            clear_up_to = list_length - safety_margin

            # Only clear if we have enough items beyond safety margin
            if clear_up_to > last_processed:
                pipe.ltrim(cache_key, clear_up_to, -1)
                # Update last processed index to where we cleared
                pipe.set(
                    last_idx_key, clear_up_to - 1
                )  # -1 because clear_up_to is kept
                pipe.expire(
                    last_idx_key,
                    get_redis_options()["redis_tick_cache"]["redis_expiry"],
                )

            results = pipe.execute()
            new_ticks = results[0]  # First result is the lrange

            # print(f"Cache key: {cache_key}")
            # print(f"Last processed: {last_processed}")
            # print(f"Current length: {list_length}")
            # print(f"New ticks: {len(new_ticks)}")
            # print(f"Cleared up to: {clear_up_to if clear_up_to > last_processed else 'none'}")

            return [json.loads(tick) for tick in new_ticks]

        except Exception as e:
            print(f"Error in get_and_clear_ticks: {e}")
            return []
