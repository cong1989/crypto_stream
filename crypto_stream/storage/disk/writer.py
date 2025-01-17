import asyncio
import fcntl
import json
import os
from pathlib import Path

from crypto_stream.configs.config import get_disk_writer_options
from crypto_stream.utils.str_utils import parse_topic


class DiskWriter:
    def __init__(self, base_dir, topic):
        self.base_dir = Path(base_dir)
        self.topic = topic
        self.exchange, self.data_type = parse_topic(topic)
        self.running = True

    def get_path_from_cache_key(self, cache_key):
        """Convert Redis cache key to filesystem path"""
        # cache_key format: crypto_ticks:exchange:type:symbol:date:hour
        parts = cache_key.split(":")[1:]  # Remove prefix

        exchange, data_type, symbol, date, hour = parts
        return self.base_dir / exchange / data_type / symbol / f"{date}.jsonl"

    def _write_ticks_to_disk(self, path, ticks):
        """Write ticks to disk with file locking"""
        print(f"Writing {len(ticks)} ticks to {path}")
        with open(path, "a") as f:
            # Get exclusive lock
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                for tick in ticks:
                    f.write(json.dumps(tick) + "\n")
                # Ensure data is written to disk
                f.flush()
                os.fsync(f.fileno())
            finally:
                # Release lock
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    async def flush_to_disk(self, cache):
        """Write cached data to disk"""
        keys = cache.get_keys_to_flush(self.exchange, self.data_type)
        print(self.exchange, self.data_type)
        print(keys)
        print("***********************************************************")
        for key in keys:
            #    print(key, 'key for cong')
            if type(key) != str:
                key = key.decode()
            ticks = cache.get_and_clear_ticks(key)
            if ticks:
                path = self.get_path_from_cache_key(key)
                path.parent.mkdir(parents=True, exist_ok=True)
                # Offload the blocking write operation to a separate thread
                await asyncio.to_thread(self._write_ticks_to_disk, path, ticks)
        # print('***********************************************************')

    async def start_flush_loop(self, cache):
        """Periodically flush cache to disk"""
        print("flush loop start")
        while self.running:
            try:
                await self.flush_to_disk(cache)
            except Exception as e:
                print(f"Error flushing to disk: {e}")
            await asyncio.sleep(get_disk_writer_options()["flush_interval"])
