import asyncio
import json
import os
from pathlib import Path
from crypto_stream.configs.config import get_disk_writer_options

class DiskWriter:
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)
        self.running = True
        
    def get_path_from_cache_key(self, cache_key):
        """Convert Redis cache key to filesystem path"""
        # cache_key format: crypto_ticks:exchange:type:symbol:date:hour
        parts = cache_key.split(':')[1:]  # Remove prefix

        exchange, data_type, symbol, date, hour = parts
        return self.base_dir / exchange / data_type / symbol / f"{date}.jsonl"
        
    def _write_ticks_to_disk(self, path, ticks):
        """Write ticks to disk in a separate thread to avoid blocking the event loop"""
        with open(path, 'a') as f:
            for tick in ticks:
                f.write(json.dumps(tick) + '\n')

    async def flush_to_disk(self, cache):
        """Write cached data to disk"""
        keys = cache.get_keys_to_flush()
        for key in keys:
            if type(key) != str:
                key = key.decode()
            ticks = cache.get_and_clear_ticks(key)
            if ticks:
                path = self.get_path_from_cache_key(key)
                path.parent.mkdir(parents=True, exist_ok=True)
                
                # Offload the blocking write operation to a separate thread
                await asyncio.to_thread(self._write_ticks_to_disk, path, ticks)

    async def start_flush_loop(self, cache):
        """Periodically flush cache to disk"""
        print("flush loop start")
        while self.running:
            try:
                await self.flush_to_disk(cache)
            except Exception as e:
                print(f"Error flushing to disk: {e}")
            await asyncio.sleep(get_disk_writer_options()['flush_interval'])