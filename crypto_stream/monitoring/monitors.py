import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Set

import pandas as pd

os.environ["TZ"] = "UTC"


class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            s = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return s


# Base monitor class to share common logging setup
class BaseMonitor:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        # Set up formatter for this logger
        formatter = UTCFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        for handler in self.logger.handlers:
            handler.setFormatter(formatter)
        # If logger has no handlers, add a NullHandler to avoid "No handlers found" warning
        if not self.logger.handlers:
            self.logger.addHandler(logging.NullHandler())


@dataclass
class SymbolStats:
    ticks_received: int = 0
    samples_created: int = 0
    last_tick_time: Optional[pd.Timestamp] = None
    last_sample_time: Optional[pd.Timestamp] = None
    errors: int = 0
    skipped_samples: int = 0


@dataclass
class SamplingStats:
    processed_ticks: int = 0
    sampled_minutes: Set[str] = field(default_factory=set)
    errors: int = 0
    skipped_minutes: int = 0
    symbol_stats: Dict[str, SymbolStats] = field(default_factory=dict)
    last_stats_print: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class SamplingMonitor(BaseMonitor):
    def __init__(self):
        super().__init__("SamplingMonitor")
        self.stats = SamplingStats()
        self.timing_tracker = TimingTracker()

    def track_tick(self, symbol: str, tick_time: pd.Timestamp):
        """Track received tick"""
        self.stats.processed_ticks += 1

        if symbol not in self.stats.symbol_stats:
            self.stats.symbol_stats[symbol] = SymbolStats()

        symbol_stat = self.stats.symbol_stats[symbol]
        symbol_stat.ticks_received += 1
        symbol_stat.last_tick_time = tick_time

        self._check_print_stats()

    def track_sample(self, symbol: str, sample_time: pd.Timestamp):
        """Track successful sample"""
        time_str = sample_time.strftime("%Y-%m-%d %H:%M")
        self.stats.sampled_minutes.add(time_str)

        if symbol in self.stats.symbol_stats:
            symbol_stat = self.stats.symbol_stats[symbol]
            symbol_stat.samples_created += 1
            symbol_stat.last_sample_time = sample_time

    def track_error(self, error_type: str, symbol: str, details: str):
        """Track error with context"""
        self.stats.errors += 1
        if symbol in self.stats.symbol_stats:
            self.stats.symbol_stats[symbol].errors += 1

        self.logger.error(f"Error for {symbol} - {error_type}: {details}")

    def track_skipped_minute(self, symbol: str, minute: pd.Timestamp, reason: str):
        """Track skipped sampling minute"""
        self.stats.skipped_minutes += 1
        if symbol in self.stats.symbol_stats:
            self.stats.symbol_stats[symbol].skipped_samples += 1

        self.logger.warning(f"Skipped sample for {symbol} at {minute}: {reason}")

    def _check_print_stats(self):
        """Print stats if enough time has passed"""
        now = datetime.now(timezone.utc)
        if (now - self.stats.last_stats_print).total_seconds() >= 60:
            self.print_stats()
            self.stats.last_stats_print = now

    def print_stats(self):
        """Print current statistics"""
        self.logger.info("\n=== Sampling Statistics ===")
        self.logger.info(f"Total Ticks: {self.stats.processed_ticks}")
        self.logger.info(f"Total Samples: {len(self.stats.sampled_minutes)}")
        self.logger.info(f"Total Errors: {self.stats.errors}")
        self.logger.info(f"Skipped Minutes: {self.stats.skipped_minutes}")

        self.logger.info("\nPer-Symbol Statistics:")
        for symbol, stats in self.stats.symbol_stats.items():
            self.logger.info(
                f"{symbol}: Ticks={stats.ticks_received}, "
                f"Samples={stats.samples_created}, "
                f"Errors={stats.errors}, "
                f"Last Tick={stats.last_tick_time}, "
                f"Last Sample={stats.last_sample_time}"
            )


class TimingTracker(BaseMonitor):
    def __init__(self):
        super().__init__("TimeTracker")
        self.timings = {}

    def start(self, operation: str):
        self.timings[operation] = time.time()

    def end(self, operation: str):
        if operation in self.timings:
            duration = time.time() - self.timings[operation]
            self.logger.debug(f"{operation}: {duration:.3f}s")
            del self.timings[operation]


class RedisMonitor(BaseMonitor):
    def __init__(self, redis_client):
        self.redis = redis_client
        super().__init__("RedisMonitor")

    async def check_health(self):
        try:
            info = self.redis.info()
            keys_info = self._get_keys_info()

            self.logger.info("\n=== Redis Health ===")
            self.logger.info(f"Memory: {info['used_memory_human']}")
            self.logger.info(f"Peak Memory: {info['used_memory_peak_human']}")
            self.logger.info(f"Connected Clients: {info['connected_clients']}")
            self.logger.info("\nKey Statistics:")
            for key_type, count in keys_info.items():
                self.logger.info(f"{key_type}: {count}")

        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")

    def _get_keys_info(self):
        return {
            "tick_buffer_keys": len(self.redis.keys("crypto_ticks_sample:*")),
            "sampled_keys": len(self.redis.keys("sampled:*")),
        }
