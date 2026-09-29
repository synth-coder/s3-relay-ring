"""
Main CLI entrypoint for Telegram S3 Gateway Daemon.
Usage:
  python3 -m tg_s3.cli run --port 9000
  python3 -m tg_s3.cli prewarm
  python3 -m tg_s3.cli drain
"""
import os
import sys
import argparse
import asyncio
import logging
from aiohttp import web

from tg_s3.db import MetadataDB
from tg_s3.staging import StagingManager
from tg_s3.mtproto import MTProtoStorageEngine
from tg_s3.server import S3Server

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("tg_s3.cli")

def parse_args():
    parser = argparse.ArgumentParser(description="Telegram-backed S3 Storage Gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run command
    run_parser = subparsers.add_parser("run", help="Start S3 Gateway server")
    run_parser.add_argument("--host", default="127.0.0.1", help="Binding host")
    run_parser.add_argument("--port", type=int, default=9000, help="Binding port")
    run_parser.add_argument("--db-path", default="/tmp/s3_metadata.db", help="Path to SQLite metadata DB")
    run_parser.add_argument("--cache-dir", default="/tmp/s3_cache", help="Local NVMe cache directory")
    run_parser.add_argument("--staging-dir", default="/tmp/s3_staging", help="Local multipart staging directory")
    run_parser.add_argument("--access-key", default=os.getenv("S3_ACCESS_KEY", "admin"))
    run_parser.add_argument("--secret-key", default=os.getenv("S3_SECRET_KEY", "password123"))
    run_parser.add_argument("--standby", action="store_true", help="Start in standby mode without MTProto connection")

    # snapshot command
    snap_parser = subparsers.add_parser("snapshot", help="Create and pin metadata snapshot to Telegram")
    snap_parser.add_argument("--db-path", default="/tmp/s3_metadata.db")
    snap_parser.add_argument("--cycle", type=int, default=0)

    # restore command
    restore_parser = subparsers.add_parser("restore", help="Restore metadata snapshot from Telegram")
    restore_parser.add_argument("--db-path", default="/tmp/s3_metadata.db")

    return parser.parse_args()

async def async_main():
    args = parse_args()

    api_id = int(os.getenv("TG_API_ID", "0"))
    api_hash = os.getenv("TG_API_HASH", "")
    session_str = os.getenv("TG_SESSION_STRING", "")
    channel_id = int(os.getenv("TG_STORAGE_CHANNEL", "0"))

    mtproto = None
    if api_id and api_hash and session_str and channel_id and not getattr(args, "standby", False):
        mtproto = MTProtoStorageEngine(api_id, api_hash, session_str, channel_id)
        await mtproto.connect()

    if args.command == "snapshot":
        if not mtproto:
            logger.error("MTProto credentials missing for snapshot")
            sys.exit(1)
        db = MetadataDB(args.db_path)
        db.checkpoint_wal()
        await mtproto.export_and_pin_index(args.db_path, args.cycle)
        await mtproto.disconnect()
        logger.info("Snapshot pinned and MTProto disconnected successfully.")
        return

    if args.command == "restore":
        if not mtproto:
            logger.error("MTProto credentials missing for restore")
            sys.exit(1)
        await mtproto.catchup_index_from_channel(args.db_path)
        await mtproto.disconnect()
        logger.info("Restore finished and MTProto disconnected successfully.")
        return

    if args.command == "run":
        db = MetadataDB(args.db_path)

        async def _sync_callback(bucket: str, key: str, file_path: str):
            if mtproto and os.path.exists(file_path):
                meta = {
                    "bucket": bucket,
                    "key": key,
                    "size": os.path.getsize(file_path),
                    "etag": db.get_object(bucket, key).get("etag", "") if db.get_object(bucket, key) else ""
                }
                msg_id = await mtproto.upload_document(file_path, meta)
                db.add_object_part(bucket, key, 1, msg_id, meta["size"], meta["etag"])
                db.mark_synced(bucket, key)

        staging = StagingManager(
            cache_dir=args.cache_dir,
            staging_dir=args.staging_dir,
            sync_callback=_sync_callback
        )

        server = S3Server(
            db=db,
            staging=staging,
            mtproto=mtproto,
            access_key=args.access_key,
            secret_key=args.secret_key
        )

        runner = web.AppRunner(server.app)
        await runner.setup()
        site = web.TCPSite(runner, args.host, args.port)
        await site.start()
        logger.info("S3 Gateway listening on http://%s:%d", args.host, args.port)

        # Keep running until SIGINT/SIGTERM
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            if mtproto:
                await mtproto.disconnect()
            await runner.cleanup()

def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
