# services/webhit_consumer.py

import asyncio
import json
import re
from datetime import datetime, timedelta

from sqlalchemy import func, select, and_, text, insert

from config.config import settings
from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from models.impression import Impression, FinishedImpression
from models.webhit import WebHit
from utils.ip import format_ipv4_as_mapped_ipv6
from utils.timer import StepTimer

MAX_MESSAGES = 5000
INT_HEAD = re.compile(r"^\d+")


def extract_leading_int(value: str) -> int | None:
    match = INT_HEAD.match(value)
    return int(match.group(0)) if match else None


class WebhitConsumerService:
    """
    Consumes webhit messages from RabbitMQ, checks against Redis for a matching impression,
    deduplicates by site, and inserts into the database if valid.

    Flow:
    - Looks up impression ID from Redis (using client_id + IP).
    - If not found, optionally falls back to DB (7-day lookback).
    - Checks if this site_id has already been seen for this impression via Redis and DB.
    - If not a duplicate, inserts a new WebHit.
    """

    def __init__(self, redis_client, rabbitmq):

        self.db = AsyncDatabaseManager()
        self.rabbitmq = rabbitmq
        self.queue_name = "webhits_queue"
        self.timer = StepTimer()
        self.redis = redis_client

    @classmethod
    async def create(cls):
        redis_client = await get_redis_client()
        rabbitmq = AsyncRabbitMQClient()
        await rabbitmq.connect()
        return cls(redis_client, rabbitmq)

    async def start(self, run_once=True, max_messages=MAX_MESSAGES):
        logger.info("WebhitConsumerService started")
        await self.db.initialize()
        await self.rabbitmq.connect()
        try:
            if run_once:
                await self.run_once(max_messages=max_messages)
            else:
                await self.rabbitmq.consume(self.queue_name, self.process_message)
                return await asyncio.Future()
        finally:
            await self.db.close()
            await self.rabbitmq.close()
            logger.info("Connections closed")

    async def run_once(self, max_messages=MAX_MESSAGES):
        """Processes webhit messages with significantly improved network efficiency."""
        logger.info("WebhitConsumerService batch processing initiated")
        batch = []
        messages = []

        # The critical optimization: Retrieve messages in sophisticated bulk
        with self.timer.time("message_retrieval"):
            # Ensure connection exists
            if not self.rabbitmq.channel:
                await self.rabbitmq.connect()

            # Get queue reference once, not repeatedly
            queue = await self.rabbitmq.channel.declare_queue(
                self.queue_name, passive=True, durable=True
            )

            # Set prefetch for optimal throughput
            await self.rabbitmq.channel.set_qos(prefetch_count=max_messages)

            # Efficient message collection with timeout
            try:
                async with queue.iterator(timeout=2.0) as queue_iter:
                    async for message in queue_iter:
                        try:
                            payload = json.loads(message.body)
                            if self._is_valid_webhit(payload):
                                batch.append(payload)
                                messages.append(message)
                                if len(batch) >= max_messages:
                                    break
                            else:
                                await message.ack()
                        except Exception as e:
                            logger.exception(f"Message parsing error: {e}")
                            await message.ack()
            except TimeoutError:
                logger.debug("Queue iterator timed out - this is normal when queue is empty or paused")

        if batch:
            # Process the batch with appropriate timing
            with self.timer.time("processing"):
                await self._handle_webhit_batch(batch)

        # Acknowledge all messages after successful processing
        with self.timer.time("acknowledgment"):
            for msg in messages:
                await msg.ack()

        count = len(batch)
        logger.info(f"Processed {count} webhit messages")
        return count

    async def process_message(self, msg):
        """
        Continuous consumption handler with appropriate elegance.
        Processes incoming webhits with singular focus.
        """
        body = msg.body if hasattr(msg, "body") else msg
        entry = json.loads(body)

        if self._is_valid_webhit(entry):
            # Extract necessary parameters for processing
            q = entry["query"]
            logger.debug(f"Processing webhit q: {q}")
            raw_ip = entry.get("client_ip", "").split(",")[0].strip()
            ip = format_ipv4_as_mapped_ipv6(raw_ip)
            client_id = int(q["client"])
            site_id = extract_leading_int(q["site"])
            logger.debug(f"[PROCESS MESSAGE] Processing webhit {site_id=} {client_id=} {ip=}")
            if site_id is None:
                logger.warning(f"Invalid site ID: {q['site']} — skipping")
                logger.warning(f"Query string was {q}")
                return

            try:
                timestmp = datetime.fromisoformat(entry.get("timestamp"))
            except Exception:
                timestmp = func.now()

            # Check Redis for impression ID
            redis_imp_id = await self.redis.get(f"imp:{client_id}:{ip}")

            # Process with our optimized implementation
            async with self.db.async_session_scope('TVFACTORY') as db:
                await self._process_single_webhit_impl(db, client_id, ip, site_id, timestmp, redis_imp_id)

        await msg.ack()

    @staticmethod
    def _is_valid_webhit(entry: dict) -> bool:
        """
        Checks if the entry contains a valid webhit query:
        requires 'client' and 'site' keys.
        """
        q = entry.get("query", {})
        logger.debug(f"[IS VALID WEBHIT] Query string was {q} and was {all(k in q for k in ('client', 'site') if q)}")
        return all(k in q for k in ("client", "site"))

    async def _handle_webhit_batch(self, entries: list[dict]):
        """
        Processes webhits in efficient batches with pipelined Redis operations.
        """
        # Phase 1: Gather impression IDs from Redis in one operation
        redis_keys = []
        entry_data = []

        with self.timer.time("prepare_batch"):
            for entry in entries:
                q = entry["query"]
                raw_ip = entry.get("client_ip", "").split(",")[0].strip()
                ip = format_ipv4_as_mapped_ipv6(raw_ip)
                logger.debug(f"[PREPARE BATCH] Processing webhit q: {q} ip is {ip}")
                client_id = int(q["client"])
                site_id = extract_leading_int(q["site"])
                ts = entry.get("timestamp")
                logger.debug(
                    f"[PREPARE BATCH AFTER SITE EXTRACT] Processing webhit {site_id=} {client_id=} {ip=} {ts=}")
                if site_id is None:
                    logger.warning(f"Invalid site ID: {q['site']} — skipping")
                    return

                try:
                    timestmp = datetime.fromisoformat(entry.get("timestamp"))
                except Exception:
                    timestmp = func.now()
                logger.debug(f"[PREPARE BATCH] Processing webhit {site_id=} {client_id=} {ip=} {timestmp=}")
                redis_keys.append(f"imp:{client_id}:{ip}")
                entry_data.append((entry, client_id, ip, site_id, timestmp))

        # Phase 2: Efficient Redis lookups with pipelining
        with self.timer.time("redis_lookup"):
            pipe = self.redis.pipeline()
            for key in redis_keys:
                pipe.get(key)
            impression_results = await pipe.execute()

        # Phase 3: Process entries with DB lookups and insertions
        with self.timer.time("db_processing"):
            async with self.db.async_session_scope('TVFACTORY') as db:
                for (entry, client_id, ip, site_id, timestmp), redis_imp_id in zip(entry_data, impression_results):
                    await self._process_single_webhit_impl(db, client_id, ip, site_id, timestmp, redis_imp_id)

    async def _process_single_webhit(self, entry, db):
        """Compatibility method for handling entry directly."""
        q = entry["query"]
        raw_ip = entry.get("client_ip", "").split(",")[0].strip()
        ip = format_ipv4_as_mapped_ipv6(raw_ip)
        client_id = int(q["client"])
        site_id = extract_leading_int(q["site"])
        if site_id is None:
            logger.warning(f"Invalid site ID: {q['site']} — skipping")
            return
        try:
            timestmp = datetime.fromisoformat(entry.get("timestamp"))
        except Exception:
            timestmp = func.now()
        redis_imp_id = await self.redis.get(f"imp:{client_id}:{ip}")

        await self._process_single_webhit_impl(db, client_id, ip, site_id, timestmp, redis_imp_id)

    async def _process_single_webhit_impl(self, db, client_id, ip, site_id, timestmp, redis_imp_id):
        """Process a single webhit with optimized database and Redis operations."""
        impression_id = int(redis_imp_id) if redis_imp_id else None
        # ip = format_ipv4_as_mapped_ipv6(ip)
        seven_days_ago = timestmp - timedelta(days=7)

        logger.debug(
            f"[PROCESS SINGLE] Processing webhit for client {client_id}, site {site_id}, IP {ip} with impression ID {impression_id}")
        # DB lookup if necessary
        row = None
        if not impression_id:
            try:
                with self.timer.time("db_impression_lookup"):
                    result = await db.execute(
                        select(Impression.id).where(
                            and_(
                                Impression.client_id == client_id,
                                Impression.ipaddress == ip,
                                Impression.timestmp > seven_days_ago,
                            )
                        ).order_by(Impression.timestmp.desc()).limit(1)
                    )
                    row = result.scalars().first()

                    if not row:
                        result = await db.execute(
                            select(FinishedImpression.id).where(
                                and_(
                                    FinishedImpression.client_id == client_id,
                                    FinishedImpression.ipaddress == ip,
                                    FinishedImpression.timestmp > seven_days_ago,
                                )
                            ).order_by(FinishedImpression.timestmp.desc()).limit(1)
                        )
                        row = result.scalars().first()
                        if not row:
                            return
                    impression_id = row
            except Exception as e:
                logger.warning(f"[GET IMPRESSION QUERY ERROR] {e}")
                return
        logger.debug(f"[PROCESS SINGLE] Impression lookup result: {row}")
        # Deduplication checks - Redis first, then DB if needed
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}"

        with self.timer.time("deduplication"):
            # Redis deduplication
            seen_sites = await self.redis.smembers(dedupe_key)
            logger.debug(f"[PROCESS SINGLE] Redis deduplication check: {seen_sites=}")
            if str(site_id) in seen_sites:
                logger.debug(f"[PROCESS SINGLE] Redis deduplication failed for {client_id=} {ip=} {site_id=}")
                return
            logger.debug(f"[PROCESS SINGLE] Redis deduplication passed for {client_id=} {ip=} {site_id=}")
            # DB deduplication
            result = await db.execute(
                select(WebHit.id).where(
                    and_(
                        WebHit.impression_id == impression_id,
                        WebHit.site_id == site_id,
                        WebHit.timestmp > seven_days_ago,
                    )
                )
            )
            if result.scalars().first():
                return

        # Insert webhit and update Redis
        logger.debug("[PROCESS SINGLE] Inserting new webhit")
        with self.timer.time("insertion"):
            try:
                await db.execute(
                    insert(WebHit).values(
                        impression_id=impression_id,
                        client_id=client_id,
                        site_id=site_id,
                        ipaddress=ip,
                        timestmp=timestmp,
                    )
                )
                await db.commit()
            except Exception as e:
                logger.warning(f"[INSERT ERROR] {e}")
            pipe = self.redis.pipeline()
            pipe.sadd(dedupe_key, site_id)
            pipe.expire(dedupe_key, settings.ONE_DAY)
            await pipe.execute()
