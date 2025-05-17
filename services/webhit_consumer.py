# services/webhit_consumer.py

import asyncio
import json
import re
from datetime import datetime, timedelta

from sqlalchemy import func, select, and_, insert

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from models.impression import Impression, FinishedImpression
from models.webhit import WebHit
from utils.ip import format_ipv4_as_mapped_ipv6
from utils.timer import StepTimer

MAX_MESSAGES = 5000
INT_HEAD = re.compile(r"^\d+")


def extract_leading_int(value: str) -> int | None:
    match = INT_HEAD.match(value)
    return int(match.group(0)) if match else None


class WebhitConsumerService(BaseAsyncFactory):
    """
    Consumes webhit messages from RabbitMQ, checks against Redis for a matching impression,
    deduplicates by site, and inserts into the database if valid.

    Flow:
    - Looks up impression ID from Redis (using client_id + IP).
    - If not found, optionally falls back to DB (7-day lookback).
    - Checks if this site_id has already been seen for this impression via Redis and DB.
    - If not a duplicate, inserts a new WebHit.
    """

    def __init__(self, run_once=False):
        super().__init__(require_redis=True, require_db=True, require_rabbit=True)
        self.queue_name = "raw_webhits_queue"
        self.timer = StepTimer()
        self.run_once = run_once
        logger.info("WebhitConsumerService initialized")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        if self.rabbit:
            # Ensure our queues exist
            await self.rabbit.declare_queue(self.queue_name, durable=True)
            await self.rabbit.declare_queue("resolved_webhits_queue", durable=True)
        logger.info("WebhitConsumerService setup complete")

    async def process_batch(self, batch_size=MAX_MESSAGES, timeout=30.0):
        """
        Processes a batch of webhit messages with sophisticated optimization.

        Args:
            batch_size: Maximum number of messages to process
            timeout: Maximum time to spend processing in seconds

        Returns:
            int: Number of messages processed
        """
        logger.info(f"Processing batch of webhit messages (max size: {batch_size})")
        batch = []
        messages = []
        start_time = asyncio.get_event_loop().time()

        # The critical optimization: retrieve messages in bulk
        with self.timer.time("message_retrieval"):
            # Create connection and channel if needed
            if not self.rabbit.channel:
                await self.rabbit.connect()

            # Get queue reference once
            queue = await self.rabbit.channel.declare_queue(
                self.queue_name, passive=True, durable=True
            )

            # Set prefetch to improve throughput
            await self.rabbit.channel.set_qos(prefetch_count=batch_size)

            # Batch collect with graceful timeout handling
            try:
                async with queue.iterator(timeout=2.0) as queue_iter:
                    async for message in queue_iter:
                        try:
                            # Check time limit
                            if asyncio.get_event_loop().time() - start_time > timeout:
                                logger.warning(f"Batch processing timeout reached after {len(batch)} messages")
                                break

                            payload = json.loads(message.body)
                            if self._is_valid_webhit(payload):
                                batch.append(payload)
                                messages.append(message)
                                if len(batch) >= batch_size:
                                    break
                            else:
                                await message.ack()
                        except Exception as e:
                            logger.exception(f"Message parsing error: {e}")
                            await message.ack()
            except TimeoutError:
                logger.debug("Queue iterator timed out - this is normal when queue is empty or paused")

        if not batch:
            return 0

        # Process the collected batch
        with self.timer.time("processing"):
            await self._handle_webhit_batch(batch)

        # Acknowledge all messages after successful processing
        with self.timer.time("acknowledgment"):
            for msg in messages:
                await msg.ack()

        logger.info(f"Processed {len(batch)} webhit messages")
        return len(batch)

    # Override _processing_loop to implement run_once behavior
    async def _processing_loop(self, interval_seconds, batch_size=None, **kwargs):
        """Processing loop with optional run-once behavior"""
        # Store batch_size if provided for use in process_batch
        if batch_size:
            self.batch_size = batch_size

        while self._running:
            try:
                # Check health with backoff
                if not await self._registry.check_with_backoff():
                    logger.warning("Health check failed - waiting before retry")
                    await asyncio.sleep(interval_seconds)
                    continue

                # Process a batch
                count = await self.process_batch(batch_size=self.batch_size)

                # If run_once is set and we've processed items, exit the loop
                if self.run_once and count > 0:
                    logger.info("Run-once mode: exiting after successful batch")
                    break

                # Sleep if no messages or between batches
                await asyncio.sleep(interval_seconds if count == 0 else 0.1)

            except asyncio.CancelledError:
                logger.info(f"{self.__class__.__name__} processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                await asyncio.sleep(interval_seconds)

    async def process_message(self, msg):
        """
        Continuous consumption handler for individual messages.
        Used when the service is in consumer mode rather than batch mode.
        """
        body = msg.body if hasattr(msg, "body") else msg
        entry = json.loads(body)

        if self._is_valid_webhit(entry):
            # Extract necessary parameters for processing
            q = entry["query"]
            logger.debug(f"Processing webhit q: {q}")
            raw_ip = entry.get("client_ip", "").split(",")[0].strip()

            try:
                ip = format_ipv4_as_mapped_ipv6(raw_ip)
            except ValueError:
                logger.warning(f"[SKIP] Invalid IP address: {raw_ip}")
                await msg.ack()
                return

            client_id = int(q["client"])
            site_id = extract_leading_int(q["site"])
            logger.debug(f"[PROCESS MESSAGE] Processing webhit {site_id=} {client_id=} {ip=}")
            if site_id is None:
                logger.warning(f"Invalid site ID: {q['site']} — skipping")
                logger.warning(f"Query string was {q}")
                await msg.ack()
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

    async def _process_single_webhit_impl(self, db, client_id, ip, site_id, timestmp, redis_imp_id):
        """Process a single webhit with optimized database and Redis operations."""
        impression_id = int(redis_imp_id) if redis_imp_id else None
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
                result = await db.execute(
                    insert(WebHit).values(
                        impression_id=impression_id,
                        client_id=client_id,
                        site_id=site_id,
                        ipaddress=ip,
                        timestmp=timestmp,
                    ).returning(WebHit.id)
                )
                webhit_id = result.scalar_one()
                await db.commit()

                # Publish to RabbitMQ
                payload = {
                    "id": webhit_id,
                    "impression_id": impression_id,
                    "client_id": client_id,
                    "site_id": site_id,
                    "ipaddress": ip,
                    "timestamp": timestmp.isoformat() if isinstance(timestmp, datetime) else str(timestmp),
                }

                await self.rabbit.publish(
                    exchange="",
                    routing_key="resolved_webhits_queue",
                    message=payload
                )

            except Exception as e:
                logger.warning(f"[INSERT ERROR] {e}")
                return

            # Redis deduplication update
            pipe = self.redis.pipeline()
            pipe.sadd(dedupe_key, site_id)
            pipe.expire(dedupe_key, settings.ONE_DAY)
            await pipe.execute()
