import asyncio
import json
import os
from asyncio import to_thread
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import redis

from crypto_stream.configs.config import (get_recording_options,
                                          get_redis_options,
                                          get_sampled_data_manager_options)
from crypto_stream.kafka_utils.consumer import create_kafka_consumer
from crypto_stream.monitoring.monitors import RedisMonitor, SamplingMonitor
from crypto_stream.storage.disk.writer import DiskWriter
from crypto_stream.storage.redis.tick_cache import RedisTickCache
from crypto_stream.utils.data_utils import (
    calculate_quote_spreads, format_quote_data,
    prepare_storage_quote_sampling_data)
from crypto_stream.utils.str_utils import parse_topic


class SampledDataManager:
    def __init__(self, topic, redis_client):
        self.redis = redis_client
        self.last_sampled_minute = None
        self.monitor = SamplingMonitor()
        self.redis_monitor = RedisMonitor(redis_client)
        self.last_health_check = datetime.now(timezone.utc)
        self.topic = topic
        self.exchange, self.data_type = parse_topic(topic)
        self._sampled_redis_options = get_sampled_data_manager_options()
        # Test Redis connection
        try:
            print("\nTesting Redis connection...")
            self.redis.ping()
            print("Successfully connected to Redis")

            print("Current keys in Redis:", self.redis.keys("*"))

            # Clear any existing keys for clean start
            # self.redis.flushdb()  # Commented out to preserve existing data
            # print("Cleared existing Redis data")

            # Test writing and reading a key
            test_key = "test:connection"
            self.redis.set(test_key, "test_value")
            test_value = self.redis.get(test_key)
            print(f"Test key value: {test_value}")
            self.redis.delete(test_key)

        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise
        except Exception as e:
            print(f"Error testing Redis: {e}")
            raise

        print("SampledDataManager initialized")

    def get_tick_buffer_key(self, exchange, data_type, symbol, timestamp):
        """
        Key for storing recent ticks for each symbol
        Args:
            exchange: Exchange name
            data_type: Type of data
            symbol: Trading symbol
            timestamp: Optional timestamp to get key for specific hour
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        elif isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

        minute_key = timestamp.strftime("%Y-%m-%d:%H:%M")
        return f"crypto_ticks_sample:{exchange}:{data_type}:{symbol}:{minute_key}"

    def get_sample_key(self, exchange, data_type, symbol):
        """Key for storing sampled data"""
        return f"sampled:{exchange}:{data_type}:{symbol}"

    def add_to_buffer(self, exchange, data_type, symbol, tick_data, storage_data):
        """Store ticks in a list"""
        try:
            self.monitor.timing_tracker.start("add_to_buffer")
            tick_timestamp = pd.Timestamp(tick_data["timestamps"]["event_time"])

            # Track tick
            self.monitor.track_tick(symbol, tick_timestamp)

            # Regular health check
            now = datetime.now(timezone.utc)
            if (now - self.last_health_check).total_seconds() >= 300:
                asyncio.create_task(self.redis_monitor.check_health())
                self.last_health_check = now

            buffer_key = self.get_tick_buffer_key(
                exchange, data_type, symbol, tick_timestamp
            )

            # Store in Redis list
            self.redis.lpush(buffer_key, json.dumps(storage_data))

            # Keep only last N items (e.g., last 1000 ticks)
            self.redis.ltrim(
                buffer_key,
                0,
                get_redis_options()["sampled_data_manager"]["redis_max_len"],
            )

            # Set expiry
            self.redis.expire(
                buffer_key, get_redis_options()["sampled_data_manager"]["redis_expiry"]
            )

            # Get the current minute of the tick
            current_minute = tick_timestamp.floor("min")

            # print(f"Current tick minute: {current_minute}")
            # print(f"Last sampled minute: {self.last_sampled_minute}")

            if self.last_sampled_minute is None:
                print("First tick - initializing sampling")
                self.create_samples_for_minute(current_minute)
                self.last_sampled_minute = current_minute
            elif current_minute > self.last_sampled_minute:
                print('#############################')
                print(f"New minute detected - sampling needed")
                print(f'exchange: {exchange}')
                print(f"trigger time: {tick_timestamp}", pd.Timestamp.now(tz = 'UTC'))
                print(f"Current minute: {current_minute}")
                print(f"Last sampled: {self.last_sampled_minute}")
                print('#############################')
                self.create_samples_for_minute(current_minute)
                #print('create_samples_for_minute done', pd.Timestamp.now(tz = 'UTC'))
                self.last_sampled_minute = current_minute
            else:
                pass
                # print("No sampling needed for this tick")

            self.monitor.timing_tracker.end("add_to_buffer")
        except Exception as e:
            import traceback

            trace = traceback.format_exc()
            self.monitor.track_error("buffer_add", symbol, str(e))

    def get_all_symbols(self, minute):
        """Get all active symbols from buffer keys"""
        try:
            # Get current and previous hour to handle boundary cases
            current_minute = minute.strftime("%Y-%m-%d:%H:%M")
            prev_minute = (minute - timedelta(minutes=1)).strftime("%Y-%m-%d:%H:%M")

            # Get all keys for both hours
            all_keys = set()
            for m in [current_minute, prev_minute]:
                pattern = f"crypto_ticks_sample:*:{m}"
                keys = self.redis.keys(pattern)
                all_keys.update(keys)

            print("all keys:", self.redis.keys("crypto_ticks_sample*"))

            print(f"Found keys: {all_keys}")

            symbols = set()
            for key in all_keys:
                if isinstance(key, bytes):
                    key = key.decode("utf-8")
                print(f"Processing key: {key}")

                parts = key.split(":")
                if len(parts) >= 4:  # crypto_ticks:exchange:type:symbol:date:hour
                    exchange = parts[1]
                    data_type = parts[2]
                    symbol = parts[3]
                    if exchange == self.exchange and data_type == self.data_type:
                        symbols.add((exchange, data_type, symbol))
                        print(f"Added symbol: ({exchange}, {data_type}, {symbol})")

            print(f"Found {len(symbols)} active symbols")
            print(f"Symbols: {symbols}")
            return symbols

        except Exception as e:
            print(f"Error in get_all_symbols: {e}")
            import traceback

            print(traceback.format_exc())
            return set()

    def get_last_tick_before_minute(self, exchange, data_type, symbol, minute):
        """Get the most recent tick before the given minute, checking previous hour if needed"""
        try:
            if not isinstance(minute, pd.Timestamp):
                minute = pd.Timestamp(minute)
            if minute.tzinfo is None:
                minute = minute.tz_localize("UTC")

            print(f"\nGetting last tick for {symbol} before {minute}")

            # Get keys for current and previous minute
            prev_minute = minute - pd.Timedelta(minutes=1)
            prev_key = self.get_tick_buffer_key(
                exchange, data_type, symbol, prev_minute
            )

            print(f"Checking previous hour key: {prev_key}")

            # Get ticks from the prev_minute hour
            all_ticks = []
            # for key in [current_key, prev_key]:
            last_tick = self.redis.lindex(prev_key, 0)
            if last_tick is None:
                print(f"No ticks found for {symbol}")
                return None
            last_tick = json.loads(last_tick)
            last_tick_time = pd.Timestamp(last_tick["timestamp"])
            # print(f"Found {len(all_ticks)} total ticks")
            # Filter out the latest available tick before the minute
            # assuming
            # Check time difference
            time_diff = minute - last_tick_time

            if time_diff.total_seconds() > self._sampled_redis_options["max_tick_age"]:
                print(f"Warning: Tick too old for {symbol}")
                return None
            if time_diff.total_seconds() < 0:
                print(
                    f"warming: negative sampling time difference at {minute} for {symbol}"
                )

            return last_tick

        except Exception as e:
            print(f"Error getting last tick: {e}")
            import traceback

            print(traceback.format_exc())
            return None

    def save_sample_to_disk(self, exchange, data_type, symbol, sampled_data):
        """Save sampled data to disk"""
        try:
            sample_date = pd.Timestamp(sampled_data["sampling_timestamp"]).strftime(
                "%Y-%m-%d"
            )
            base_path = (
                Path(get_recording_options()["precise_sampler_dir"])
                / "sampled"
                / exchange
                / data_type
                / symbol
            )
            base_path.mkdir(parents=True, exist_ok=True)

            file_path = base_path / f"{sample_date}_sampled.jsonl"

            print(
                f"\nSaving sample to disk: {symbol} tick timestamp {sampled_data['timestamp']}, sample time: {sampled_data['sampling_timestamp']}"
            )

            with open(file_path, "a") as f:
                json_str = json.dumps(sampled_data)
                f.write(json_str + "\n")
                f.flush()
                os.fsync(f.fileno())

        except Exception as e:
            print(f"Error saving to disk: {e}")
            import traceback

            print(traceback.format_exc())

    def get_latest_minute_sample_channel_name(self, exchange, data_type, symbol):
        """Get Redis pub/sub channel name for sample updates"""
        return f"latest_samples:{exchange}:{data_type}:{symbol}"

    def publish_sample(self, exchange, data_type, symbol, sampled_data):
        """Publish sample updates to various channels"""
        try:
            message = {
                'data': sampled_data,
                'publish_time': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            }
            message_json = json.dumps(message)
            
            # Publish to symbol-specific channel
            symbol_channel = self.get_latest_minute_sample_channel_name(exchange, data_type, symbol)
            self.redis.publish(symbol_channel, message_json)
            
            # Publish to exchange-wide channel
            exchange_channel = f"latest_samples:{exchange}:{data_type}"
            self.redis.publish(exchange_channel, message_json)
            
        except Exception as e:
            print(f"Error publishing sample: {e}")
            import traceback
            print(traceback.format_exc())

    def save_sample(self, exchange, data_type, symbol, sampled_data, minute):
        """Save the sample to Redis and disk"""
        try:
            print(f"\nSaving sample for {symbol} at {minute}")
            
            # Save to Redis
            sample_key = self.get_sample_key(exchange, data_type, symbol)
            self.redis.set(sample_key, json.dumps(sampled_data))
            
            # Publish updates
            self.publish_sample(exchange, data_type, symbol, sampled_data)
            
            # Save window
            window_key = f"{sample_key}:window"
            self.redis.rpush(window_key, json.dumps(sampled_data))
            self.redis.ltrim(window_key, 
                            -self._sampled_redis_options["number_of_minute_samples_to_keep"],
                            -1)
            
            # Save to disk
            self.save_sample_to_disk(exchange, data_type, symbol, sampled_data)
            
        except Exception as e:
            print(f"Error saving sample: {e}")
            import traceback
            print(traceback.format_exc())

    def create_samples_for_minute(self, minute):
        try:
            self.monitor.timing_tracker.start("sampling")
            symbols = self.get_all_symbols(minute)

            for exchange, data_type, symbol in symbols:
                #print(f"\nProcessing symbol: {symbol}", pd.Timestamp.now(tz = 'UTC'))
                try:
                    last_tick = self.get_last_tick_before_minute(
                        exchange, data_type, symbol, minute
                    )
                    if last_tick:
                        tick_time = pd.Timestamp(last_tick["timestamp"])
                        if tick_time < minute:
                            sampled_data = {
                                **last_tick,
                                "sampling_timestamp": minute.strftime(
                                    "%Y-%m-%dT%H:%M:00.000Z"
                                ),
                            }
                            self.save_sample(
                                exchange, data_type, symbol, sampled_data, minute
                            )
                            self.monitor.track_sample(symbol, minute)
                        else:
                            self.monitor.track_skipped_minute(
                                symbol,
                                minute,
                                f"Tick time not before sampling time!\n last tick: {last_tick}",
                            )
                    else:
                        self.monitor.track_skipped_minute(
                            symbol, minute, "No valid tick found"
                        )
                except Exception as e:
                    self.monitor.track_error("sampling", symbol, str(e))

            self.monitor.timing_tracker.end("sampling")

        except Exception as e:
            self.monitor.track_error("sampling_all", "all_symbols", str(e))
            raise


class EnhancedRedisTickCache(RedisTickCache):
    def __init__(self, topic, host="localhost", port=6379, db=0):
        super().__init__(topic, host, port, db)
        self.sampled_data = SampledDataManager(topic, self.redis)

    def add_tick(self, tick_data, storage_data):
        try:
            # Store raw tick as before
            super().add_tick(tick_data, storage_data)

            # Process for sampling
            self.sampled_data.add_to_buffer(
                tick_data["market_data"]["exchange"],
                tick_data["market_data"]["type"],
                tick_data["market_data"]["symbol"],
                tick_data,
                storage_data,
            )
        except Exception as e:
            print(f"Error in add_tick: {e}")
