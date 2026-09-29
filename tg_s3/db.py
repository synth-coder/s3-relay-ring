"""
SQLite Metadata Store for Telegram S3 Gateway.
Handles buckets, objects, multi-part chunk mappings, and dirty sync status.
Operates in WAL mode with sub-millisecond query performance.
"""
import os
import sqlite3
import time
from typing import List, Dict, Any, Optional, Tuple

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    etag TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    synced INTEGER DEFAULT 0, -- 1=synced to TG, 0=dirty in local NVMe cache
    PRIMARY KEY (bucket, key)
);

CREATE TABLE IF NOT EXISTS object_parts (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    msg_id INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (bucket, key, part_number),
    FOREIGN KEY (bucket, key) REFERENCES objects(bucket, key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_objects_lookup ON objects(bucket, key);
CREATE INDEX IF NOT EXISTS idx_objects_synced ON objects(synced);
CREATE INDEX IF NOT EXISTS idx_parts_lookup ON object_parts(bucket, key);
"""

class MetadataDB:
    def __init__(self, db_path: str = "/tmp/s3_metadata.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.executescript(SCHEMA)

    def close(self):
        if self.conn:
            self.conn.close()

    # --- Bucket Operations ---
    def create_bucket(self, bucket: str) -> bool:
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?, ?)",
                    (bucket, int(time.time()))
                )
            return True
        except Exception:
            return False

    def list_buckets(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, created_at FROM buckets ORDER BY name ASC")
        return [dict(row) for row in cursor.fetchall()]

    def delete_bucket(self, bucket: str) -> bool:
        with self.conn:
            # Check if bucket contains objects
            cur = self.conn.execute("SELECT COUNT(*) FROM objects WHERE bucket = ?", (bucket,))
            if cur.fetchone()[0] > 0:
                return False  # Bucket not empty
            self.conn.execute("DELETE FROM buckets WHERE name = ?", (bucket,))
        return True

    def head_bucket(self, bucket: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM buckets WHERE name = ?", (bucket,))
        return cur.fetchone() is not None

    # --- Object Operations ---
    def put_object(self, bucket: str, key: str, size: int, etag: str, synced: int = 0) -> None:
        self.create_bucket(bucket)
        now = int(time.time())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO objects (bucket, key, size_bytes, etag, created_at, synced)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket, key) DO UPDATE SET
                    size_bytes = excluded.size_bytes,
                    etag = excluded.etag,
                    created_at = excluded.created_at,
                    synced = excluded.synced
                """,
                (bucket, key, size, etag, now, synced)
            )

    def get_object(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT bucket, key, size_bytes, etag, created_at, synced FROM objects WHERE bucket = ? AND key = ?",
            (bucket, key)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def delete_object(self, bucket: str, key: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM object_parts WHERE bucket = ? AND key = ?", (bucket, key))
            self.conn.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (bucket, key))

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Implements AWS S3 ListObjectsV2 prefix and delimiter filtering.
        """
        cursor = self.conn.cursor()
        query = "SELECT key, size_bytes, etag, created_at FROM objects WHERE bucket = ?"
        params: List[Any] = [bucket]

        if prefix:
            query += " AND key LIKE ?"
            params.append(f"{prefix}%")

        if continuation_token:
            query += " AND key > ?"
            params.append(continuation_token)

        query += " ORDER BY key ASC"
        cursor.execute(query, params)
        rows = cursor.fetchall()

        contents = []
        common_prefixes = set()
        next_continuation_token = None
        is_truncated = False

        for row in rows:
            key = row["key"]
            if delimiter and delimiter in key[len(prefix):]:
                # Group into common prefix
                sub = key[len(prefix):]
                delim_pos = sub.index(delimiter)
                cp = prefix + sub[:delim_pos + len(delimiter)]
                common_prefixes.add(cp)
            else:
                if len(contents) < max_keys:
                    contents.append({
                        "key": key,
                        "size": row["size_bytes"],
                        "etag": row["etag"],
                        "last_modified": row["created_at"]
                    })
                else:
                    is_truncated = True
                    next_continuation_token = contents[-1]["key"] if contents else None
                    break

        return {
            "name": bucket,
            "prefix": prefix,
            "delimiter": delimiter,
            "max_keys": max_keys,
            "is_truncated": is_truncated,
            "next_continuation_token": next_continuation_token,
            "contents": contents,
            "common_prefixes": sorted(list(common_prefixes))
        }

    # --- Multipart / Parts Operations ---
    def add_object_part(self, bucket: str, key: str, part_num: int, msg_id: int, size: int, etag: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO object_parts (bucket, key, part_number, msg_id, size_bytes, etag)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (bucket, key, part_num, msg_id, size, etag)
            )

    def get_object_parts(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT part_number, msg_id, size_bytes, etag FROM object_parts WHERE bucket = ? AND key = ? ORDER BY part_number ASC",
            (bucket, key)
        )
        return [dict(r) for r in cur.fetchall()]

    def initiate_multipart(self, upload_id: str, bucket: str, key: str) -> None:
        self.create_bucket(bucket)
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO multipart_uploads (upload_id, bucket, key, created_at) VALUES (?, ?, ?, ?)",
                (upload_id, bucket, key, int(time.time()))
            )

    def get_multipart(self, upload_id: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT upload_id, bucket, key, created_at FROM multipart_uploads WHERE upload_id = ?",
            (upload_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def abort_multipart(self, upload_id: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM multipart_uploads WHERE upload_id = ?", (upload_id,))

    # --- Sync Status / Handover Flush ---
    def get_dirty_objects(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT bucket, key, size_bytes, etag, created_at FROM objects WHERE synced = 0"
        )
        return [dict(r) for r in cur.fetchall()]

    def mark_synced(self, bucket: str, key: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE objects SET synced = 1 WHERE bucket = ? AND key = ?",
                (bucket, key)
            )

    def checkpoint_wal(self) -> None:
        """Truncates WAL log to ensure index.db file contains complete state before snapshot upload."""
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
