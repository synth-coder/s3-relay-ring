"""
Local NVMe Staging & Cache Manager for S3 Gateway.
Handles:
1. Fast local read cache (/tmp/s3_cache/)
2. Multipart upload assembly buffer (/tmp/s3_staging/)
3. Debounced write-behind queue for small real-time writes
4. Auto-eviction when local scratch space reaches threshold
"""
import os
import shutil
import hashlib
import time
import asyncio
from typing import Dict, Any, Optional, Callable, Awaitable, Tuple

class StagingManager:
    def __init__(
        self,
        cache_dir: str = "/tmp/s3_cache",
        staging_dir: str = "/tmp/s3_staging",
        debounce_secs: float = 5.0,
        max_cache_bytes: int = 50 * 1024 * 1024 * 1024, # 50 GB
        sync_callback: Optional[Callable[[str, str, str], Awaitable[None]]] = None
    ):
        self.cache_dir = cache_dir
        self.staging_dir = staging_dir
        self.debounce_secs = debounce_secs
        self.max_cache_bytes = max_cache_bytes
        self.sync_callback = sync_callback

        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.staging_dir, exist_ok=True)

        # In-memory debounce queue: (bucket, key) -> asyncio.TimerHandle
        self._debounce_tasks: Dict[str, asyncio.TimerHandle] = {}

    def get_cache_path(self, bucket: str, key: str) -> str:
        # Sanitize key and verify path stays strictly within bucket cache directory
        bucket_dir = os.path.abspath(os.path.join(self.cache_dir, bucket))
        clean_key = os.path.normpath(key.lstrip("/"))
        target_path = os.path.abspath(os.path.join(bucket_dir, clean_key))
        if not target_path.startswith(bucket_dir + os.sep) and target_path != bucket_dir:
            raise ValueError(f"Illegal path traversal attempt in S3 key: {key}")
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        return target_path

    def get_multipart_part_path(self, upload_id: str, part_num: int) -> str:
        path = os.path.join(self.staging_dir, upload_id, f"part_{part_num:05d}.bin")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def write_cache_stream(self, bucket: str, key: str) -> Tuple[str, Any]:
        """Returns target file path and an open file descriptor in write mode."""
        path = self.get_cache_path(bucket, key)
        return path, open(path, "wb")

    def has_cached(self, bucket: str, key: str) -> bool:
        path = self.get_cache_path(bucket, key)
        return os.path.isfile(path) and os.path.getsize(path) > 0

    def get_cached_size(self, bucket: str, key: str) -> int:
        path = self.get_cache_path(bucket, key)
        return os.path.getsize(path) if os.path.isfile(path) else 0

    def schedule_sync(self, bucket: str, key: str, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Schedules a debounced background sync of a locally written object to Telegram.
        If written multiple times within debounce_secs, earlier sync is canceled.
        """
        item_id = f"{bucket}/{key}"
        if item_id in self._debounce_tasks:
            self._debounce_tasks[item_id].cancel()

        if loop is None:
            loop = asyncio.get_event_loop()

        def _trigger():
            if self.sync_callback:
                path = self.get_cache_path(bucket, key)
                asyncio.ensure_future(self.sync_callback(bucket, key, path))
            self._debounce_tasks.pop(item_id, None)

        handle = loop.call_later(self.debounce_secs, _trigger)
        self._debounce_tasks[item_id] = handle

    async def flush_all_pending(self):
        """
        Forces immediate execution of all debounced writes before shift handover.
        """
        pending = list(self._debounce_tasks.items())
        for item_id, handle in pending:
            handle.cancel()
            bucket, key = item_id.split("/", 1)
            path = self.get_cache_path(bucket, key)
            if self.sync_callback and os.path.isfile(path):
                await self.sync_callback(bucket, key, path)
        self._debounce_tasks.clear()

    def assemble_multipart(self, upload_id: str, target_path: str) -> Tuple[int, str]:
        """
        Aggregates all buffered parts into the target output file.
        Computes final size and MD5 ETag.
        """
        upload_dir = os.path.join(self.staging_dir, upload_id)
        if not os.path.isdir(upload_dir):
            raise FileNotFoundError(f"Upload directory for {upload_id} not found")

        part_files = sorted(os.listdir(upload_dir))
        md5 = hashlib.md5()
        total_bytes = 0

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as out_f:
            for pf in part_files:
                part_path = os.path.join(upload_dir, pf)
                with open(part_path, "rb") as in_f:
                    while chunk := in_f.read(1024 * 1024):
                        out_f.write(chunk)
                        md5.update(chunk)
                        total_bytes += len(chunk)

        # Cleanup staging parts
        shutil.rmtree(upload_dir, ignore_errors=True)
        return total_bytes, md5.hexdigest()

    def abort_multipart_staging(self, upload_id: str) -> None:
        upload_dir = os.path.join(self.staging_dir, upload_id)
        shutil.rmtree(upload_dir, ignore_errors=True)

    def evict_if_needed(self):
        """Purges oldest cached files if scratch disk exceeds max_cache_bytes."""
        total_size = 0
        file_list = []
        for root, _, files in os.walk(self.cache_dir):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    stat = os.stat(fp)
                    total_size += stat.st_size
                    file_list.append((stat.st_mtime, stat.st_size, fp))
                except OSError:
                    pass

        if total_size > self.max_cache_bytes:
            # Sort oldest first
            file_list.sort(key=lambda x: x[0])
            for mtime, size, fp in file_list:
                try:
                    os.remove(fp)
                    total_size -= size
                    if total_size <= self.max_cache_bytes * 0.75: # Down to 75%
                        break
                except OSError:
                    pass
