"""
High-Throughput Parallel MTProto Transfer Engine for Telethon.
Multiplexes chunk uploads across parallel MTProtoSender TCP streams to the primary DC.
Implements the canonical parallel streaming transfer pattern (mautrix-telegram / FastTelethon).
"""
import os
import copy
import asyncio
import logging
from typing import Optional, List
from telethon import TelegramClient, helpers
from telethon.tl import functions, types
from telethon.tl.alltlobjects import LAYER
from telethon.network import MTProtoSender
from telethon.errors import FloodWaitError

logger = logging.getLogger("tg_s3.fast_mtproto")

class FastUploadSender:
    def __init__(self, client: TelegramClient, sender: MTProtoSender, file_id: int, part_count: int, is_big: bool, stride: int, offset: int):
        self.client = client
        self.sender = sender
        self.file_id = file_id
        self.part_count = part_count
        self.is_big = is_big
        self.stride = stride
        self.part_index = offset
        self.loop = asyncio.get_event_loop()
        self.previous_task: Optional[asyncio.Task] = None

    async def _send_part(self, data: bytes, part_num: int):
        if self.is_big:
            req = functions.upload.SaveBigFilePartRequest(
                file_id=self.file_id,
                file_part=part_num,
                file_total_parts=self.part_count,
                bytes=data
            )
        else:
            req = functions.upload.SaveFilePartRequest(
                file_id=self.file_id,
                file_part=part_num,
                bytes=data
            )
        # Use Telethon's internal sender invocation with retry
        for attempt in range(3):
            try:
                await self.client._call(self.sender, req)
                return
            except FloodWaitError as e:
                logger.warning("FloodWait on sender part %d: waiting %ds", part_num, e.seconds)
                await asyncio.sleep(min(e.seconds + 1, 60))
            except Exception as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(1)

    async def enqueue(self, data: bytes):
        current_part = self.part_index
        self.part_index += self.stride

        # Pipeline: wait for prior chunk on this connection before scheduling next
        if self.previous_task:
            await self.previous_task
        self.previous_task = self.loop.create_task(self._send_part(data, current_part))

    async def wait_complete(self):
        if self.previous_task:
            await self.previous_task
            self.previous_task = None


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: int, workers: int = 4):
        self.client = client
        self.dc_id = dc_id
        self.workers = max(1, min(workers, 8))
        self.raw_senders: List[MTProtoSender] = []

    async def init_senders(self):
        """Creates parallel MTProtoSender TCP streams to the target DC."""
        dc = await self.client._get_dc(self.dc_id)

        # Primary connection sender can be used as sender 0 if on same DC
        # For additional parallel streams, we create new MTProtoSender instances
        for i in range(self.workers):
            sender = MTProtoSender(None, loggers=self.client._log)
            await sender.connect(self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
                local_addr=self.client._local_addr
            ))
            # Export authorization from primary session and import into secondary sender
            auth = await self.client(functions.auth.ExportAuthorizationRequest(self.dc_id))
            # M1 Audit Fix: Clone client._init_request to avoid mutating shared primary client state
            init_req = copy.copy(self.client._init_request)
            init_req.query = functions.auth.ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            layer_req = functions.InvokeWithLayerRequest(LAYER, init_req)
            await sender.send(layer_req)
            self.raw_senders.append(sender)

    async def close(self):
        """Closes all parallel sender TCP connections."""
        for sender in self.raw_senders:
            try:
                await sender.disconnect()
            except Exception:
                pass
        self.raw_senders.clear()


async def fast_upload_file(
    client: TelegramClient,
    file_path: str,
    workers: int = 4,
    part_size_kb: int = 512
) -> types.TypeInputFile:
    """
    Uploads a file using parallel MTProtoSender streams.
    Returns InputFileBig or InputFile ready for client.send_file.
    """
    file_size = os.path.getsize(file_path)
    part_size = part_size_kb * 1024
    part_count = (file_size + part_size - 1) // part_size
    is_big = file_size > 10 * 1024 * 1024
    file_id = helpers.generate_random_long()
    file_name = os.path.basename(file_path)

    # Determine DC of the current session directly without extra network RPC
    if not client.session or not client.session.dc_id:
        raise RuntimeError("Telegram client session missing DC ID")
    dc_id = client.session.dc_id

    actual_workers = min(workers, part_count)
    transferrer = ParallelTransferrer(client, dc_id, workers=actual_workers)

    try:
        await transferrer.init_senders()
        num_senders = len(transferrer.raw_senders)

        uploaders = [
            FastUploadSender(
                client=client,
                sender=transferrer.raw_senders[i],
                file_id=file_id,
                part_count=part_count,
                is_big=is_big,
                stride=num_senders,
                offset=i
            )
            for i in range(num_senders)
        ]

        # Read and dispatch parts round-robin across senders
        ticker = 0
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break
                await uploaders[ticker].enqueue(chunk)
                ticker = (ticker + 1) % num_senders

        # Await completion of all pipelined tasks
        await asyncio.gather(*[u.wait_complete() for u in uploaders])

        if is_big:
            return types.InputFileBig(id=file_id, parts=part_count, name=file_name)
        else:
            # MD5 calculation for standard InputFile
            import hashlib
            with open(file_path, "rb") as f:
                md5_hex = hashlib.md5(f.read()).hexdigest()
            return types.InputFile(id=file_id, parts=part_count, name=file_name, md5_checksum=md5_hex)

    finally:
        await transferrer.close()
