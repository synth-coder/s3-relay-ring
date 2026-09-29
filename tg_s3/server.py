"""
Async S3 REST API Server built on aiohttp.
Exposes standard AWS S3 REST endpoints on 127.0.0.1:9000:
- ListBuckets, HeadBucket, CreateBucket, DeleteBucket
- ListObjectsV2, HeadObject, GetObject (with Range support), PutObject, DeleteObject
- Multipart Uploads (Initiate, UploadPart, CompleteMultipartUpload, AbortMultipartUpload)
- Handover Drain Mode (returns 503 Retry-After: 5 during rotation)
"""
import os
import re
import time
import hashlib
import logging
from aiohttp import web
from typing import Optional

from tg_s3.db import MetadataDB
from tg_s3.staging import StagingManager
from tg_s3.mtproto import MTProtoStorageEngine

logger = logging.getLogger("tg_s3.server")

class S3Server:
    def __init__(
        self,
        db: MetadataDB,
        staging: StagingManager,
        mtproto: Optional[MTProtoStorageEngine] = None,
        access_key: str = "admin",
        secret_key: str = "password123"
    ):
        self.db = db
        self.staging = staging
        self.mtproto = mtproto
        self.access_key = access_key
        self.secret_key = secret_key
        self.is_draining = False
        self._inflight_puts = 0
        self.drain_token = os.getenv("DRAIN_TOKEN", "")

        self.app = web.Application(client_max_size=10 * 1024 * 1024 * 1024) # 10 GB
        self._setup_routes()

    def _setup_routes(self):
        # Health & Internal Handover Controls
        self.app.router.add_get("/healthz", self.handle_healthz)
        self.app.router.add_post("/internal/drain", self.handle_drain)

        # S3 Root Operations
        self.app.router.add_get("/", self.handle_root_get)

        # S3 Bucket & Object Operations (Note: in aiohttp add_get automatically handles HEAD unless overridden)
        self.app.router.add_get("/{bucket}", self.handle_bucket_get, allow_head=False)
        self.app.router.add_head("/{bucket}", self.handle_bucket_head)
        self.app.router.add_put("/{bucket}", self.handle_bucket_put)
        self.app.router.add_delete("/{bucket}", self.handle_bucket_delete)

        # Wildcard object routes (matches any nested key)
        self.app.router.add_get("/{bucket}/{key:.*}", self.handle_object_get, allow_head=False)
        self.app.router.add_head("/{bucket}/{key:.*}", self.handle_object_head)
        self.app.router.add_put("/{bucket}/{key:.*}", self.handle_object_put)
        self.app.router.add_post("/{bucket}/{key:.*}", self.handle_object_post)
        self.app.router.add_delete("/{bucket}/{key:.*}", self.handle_object_delete)

    # --- Health & Drain Control ---
    async def handle_healthz(self, request: web.Request) -> web.Response:
        return web.Response(text="OK", status=200)

    async def handle_drain(self, request: web.Request) -> web.Response:
        """Toggles drain mode for pre-handover cutover."""
        # Fix C6: Require Authorization Bearer DRAIN_TOKEN unconditionally.
        if not self.drain_token:
            logger.error("Drain requested but DRAIN_TOKEN is not configured!")
            return web.Response(status=500, text="Drain Token Not Configured")

        auth_header = request.headers.get("Authorization", "")
        expected = f"Bearer {self.drain_token}"
        if auth_header != expected:
            return web.Response(status=403, text="Forbidden")

        self.is_draining = True
        logger.info("DRAIN MODE ACTIVATED: Awaiting in-flight writes before flush...")
        while self._inflight_puts > 0:
            await asyncio.sleep(0.05)

        await self.staging.flush_all_pending()
        logger.info("DRAIN MODE: All writes completed and flushed cleanly.")
        return web.Response(text="DRAINING", status=200)

    # --- Root Handlers ---
    async def handle_root_get(self, request: web.Request) -> web.Response:
        """ListBuckets"""
        buckets = self.db.list_buckets()
        xml_buckets = "".join(
            f"<Bucket><Name>{b['name']}</Name><CreationDate>{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(b['created_at']))}</CreationDate></Bucket>"
            for b in buckets
        )
        body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner><ID>tgs3</ID><DisplayName>tgs3</DisplayName></Owner>
    <Buckets>{xml_buckets}</Buckets>
</ListAllMyBucketsResult>"""
        return web.Response(text=body, content_type="application/xml")

    # --- Bucket Handlers ---
    async def handle_bucket_put(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        self.db.create_bucket(bucket)
        return web.Response(status=200)

    async def handle_bucket_head(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        if self.db.head_bucket(bucket):
            return web.Response(status=200)
        return web.Response(status=404)

    async def handle_bucket_delete(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        if self.db.delete_bucket(bucket):
            return web.Response(status=204)
        return web.Response(status=409, text="BucketNotEmpty")

    async def handle_bucket_get(self, request: web.Request) -> web.Response:
        """ListObjectsV2"""
        bucket = request.match_info["bucket"]
        if not self.db.head_bucket(bucket):
            return web.Response(status=404, text="NoSuchBucket")

        prefix = request.query.get("prefix", "")
        delimiter = request.query.get("delimiter", "")
        max_keys = int(request.query.get("max-keys", 1000))
        continuation_token = request.query.get("continuation-token")

        res = self.db.list_objects_v2(bucket, prefix, delimiter, max_keys, continuation_token)

        xml_contents = "".join(
            f"""<Contents>
    <Key>{c['key']}</Key>
    <LastModified>{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(c['last_modified']))}</LastModified>
    <ETag>&quot;{c['etag']}&quot;</ETag>
    <Size>{c['size']}</Size>
    <StorageClass>STANDARD</StorageClass>
</Contents>"""
            for c in res["contents"]
        )

        xml_prefixes = "".join(
            f"<CommonPrefixes><Prefix>{cp}</Prefix></CommonPrefixes>"
            for cp in res["common_prefixes"]
        )

        body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{bucket}</Name>
    <Prefix>{prefix}</Prefix>
    <MaxKeys>{max_keys}</MaxKeys>
    <IsTruncated>{str(res['is_truncated']).lower()}</IsTruncated>
    {xml_contents}
    {xml_prefixes}
</ListBucketResult>"""
        return web.Response(text=body, content_type="application/xml")

    # --- Object Handlers ---
    async def handle_object_head(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        key = request.match_info["key"]

        obj = self.db.get_object(bucket, key)
        if not obj:
            return web.Response(status=404)

        headers = {
            "Content-Length": str(obj["size_bytes"]),
            "ETag": f'"{obj["etag"]}"',
            "Accept-Ranges": "bytes",
            "Last-Modified": time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(obj["created_at"]))
        }
        return web.Response(status=200, headers=headers)

    async def handle_object_get(self, request: web.Request) -> web.StreamResponse:
        bucket = request.match_info["bucket"]
        key = request.match_info["key"]

        obj = self.db.get_object(bucket, key)
        if not obj:
            return web.Response(status=404)

        total_size = obj["size_bytes"]
        range_header = request.headers.get("Range")

        start, end = 0, total_size - 1
        is_range = False
        if range_header and range_header.startswith("bytes="):
            match = re.match(r"bytes=(\d*)-(\d*)", range_header)
            if match:
                s_str, e_str = match.groups()
                start = int(s_str) if s_str else 0
                end = int(e_str) if e_str else total_size - 1
                is_range = True

        content_length = (end - start) + 1
        status = 206 if is_range else 200

        resp = web.StreamResponse(status=status)
        resp.headers["Content-Length"] = str(content_length)
        resp.headers["ETag"] = f'"{obj["etag"]}"'
        resp.headers["Accept-Ranges"] = "bytes"
        if is_range:
            resp.headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

        await resp.prepare(request)

        # Path 1: Local NVMe Cache Hit
        if self.staging.has_cached(bucket, key):
            cache_file = self.staging.get_cache_path(bucket, key)
            with open(cache_file, "rb") as f:
                f.seek(start)
                remaining = content_length
                while remaining > 0:
                    chunk = f.read(min(remaining, 1024 * 1024))
                    if not chunk:
                        break
                    await resp.write(chunk)
                    remaining -= len(chunk)
            await resp.write_eof()
            return resp

        # Path 2: Stream from MTProto
        parts = self.db.get_object_parts(bucket, key)
        if self.mtproto and parts:
            part = parts[0] # Single part or seek logic
            async for chunk in self.mtproto.stream_file_range(part["msg_id"], start, content_length):
                await resp.write(chunk)
            await resp.write_eof()
            return resp

        return web.Response(status=404)

    async def handle_object_put(self, request: web.Request) -> web.Response:
        """Single-shot PUT or Multipart Part Upload"""
        self._inflight_puts += 1
        try:
            if self.is_draining:
                # Drain window gatekeeper
                return web.Response(status=503, headers={"Retry-After": "5"}, text="Service Draining")

            bucket = request.match_info["bucket"]
            key = request.match_info["key"]

            # Check if this is an S3 UploadPart: ?uploadId=...&partNumber=...
            upload_id = request.query.get("uploadId")
            part_num = request.query.get("partNumber")

            if upload_id and part_num:
                part_path = self.staging.get_multipart_part_path(upload_id, int(part_num))
                md5 = hashlib.md5()
                total_bytes = 0
                with open(part_path, "wb") as f:
                    while True:
                        chunk = await request.content.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        md5.update(chunk)
                        total_bytes += len(chunk)

                etag = md5.hexdigest()
                return web.Response(status=200, headers={"ETag": f'"{etag}"'})

            # Standard S3 PutObject
            cache_path, f = self.staging.write_cache_stream(bucket, key)
            md5 = hashlib.md5()
            total_bytes = 0
            try:
                while True:
                    chunk = await request.content.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    md5.update(chunk)
                    total_bytes += len(chunk)
            finally:
                f.close()

            etag = md5.hexdigest()
            self.db.put_object(bucket, key, total_bytes, etag, synced=0)

            # Schedule debounced MTProto background sync
            self.staging.schedule_sync(bucket, key)

            return web.Response(status=200, headers={"ETag": f'"{etag}"'})
        finally:
            self._inflight_puts -= 1

    async def handle_object_post(self, request: web.Request) -> web.Response:
        """Multipart Upload Control: Initiate or Complete"""
        self._inflight_puts += 1
        try:
            if self.is_draining:
                return web.Response(status=503, headers={"Retry-After": "5"}, text="Service Draining")

            bucket = request.match_info["bucket"]
            key = request.match_info["key"]

            # 1. InitiateMultipartUpload: ?uploads
            if "uploads" in request.query:
                upload_id = hashlib.sha256(f"{bucket}/{key}/{time.time()}".encode()).hexdigest()[:24]
                self.db.initiate_multipart(upload_id, bucket, key)
                body = f"""<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{bucket}</Bucket>
    <Key>{key}</Key>
    <UploadId>{upload_id}</UploadId>
</InitiateMultipartUploadResult>"""
                return web.Response(text=body, content_type="application/xml")

            # 2. CompleteMultipartUpload: ?uploadId=...
            upload_id = request.query.get("uploadId")
            if upload_id:
                target_cache_path = self.staging.get_cache_path(bucket, key)
                total_bytes, etag = self.staging.assemble_multipart(upload_id, target_cache_path)
                self.db.put_object(bucket, key, total_bytes, etag, synced=0)
                self.db.abort_multipart(upload_id)

                # Schedule debounced MTProto sync
                self.staging.schedule_sync(bucket, key)

                body = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{bucket}</Bucket>
    <Key>{key}</Key>
    <ETag>&quot;{etag}&quot;</ETag>
</CompleteMultipartUploadResult>"""
                return web.Response(text=body, content_type="application/xml")

            return web.Response(status=400)
        finally:
            self._inflight_puts -= 1

    async def handle_object_delete(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        key = request.match_info["key"]

        # Abort Multipart if uploadId passed
        upload_id = request.query.get("uploadId")
        if upload_id:
            self.staging.abort_multipart_staging(upload_id)
            self.db.abort_multipart(upload_id)
            return web.Response(status=204)

        self.db.delete_object(bucket, key)
        cache_path = self.staging.get_cache_path(bucket, key)
        if os.path.exists(cache_path):
            os.remove(cache_path)

        # Publish delete tombstone to Telegram so successors don't resurrect it
        if self.mtproto:
            asyncio.ensure_future(self.mtproto.record_delete(bucket, key))

        return web.Response(status=204)
