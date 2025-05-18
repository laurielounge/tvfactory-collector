# services/webhit_consumer.py

import asyncio
import json
import re
import time
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert as mysql_insert

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from models.last_site_response import TargetLastSiteResponse
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
        # Site activity tracking
        self.site_hit_counters = {}  # Format: {(date, client_id, site_id): count}
        self.last_counter_flush = time.time()
        self.counter_flush_interval = 60  # Seconds
        logger.info("WebhitConsumerService initialized with site activity tracking")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        if self.rabbit:
            # Ensure our queues exist
            await self.rabbit.declare_queue(self.queue_name, durable=True)
            await self.rabbit.declare_queue("resolved_webhits_queue", durable=True)
        logger.info("WebhitConsumerService setup complete")

    # Override stop method to ensure counters are flushed
    async def stop(self):
        """Gracefully stops the service and flushes remaining counters."""
        if self.site_hit_counters:
            await self.flush_site_hit_counters()
        await super().stop()

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
        Processes webhits in efficient batches with timestamp-based processing control.
        """
        # Get latest processed impression timestamp first
        latest_imp_ts = await self.redis.get("latest_impression_timestamp")
        if not latest_imp_ts:
            latest_imp_ts = 0
        else:
            latest_imp_ts = float(latest_imp_ts)

        logger.info(
            f"Latest impression timestamp: {datetime.fromtimestamp(float(latest_imp_ts)).isoformat() if latest_imp_ts else None}")

        # Update site hit counters
        today = datetime.now().date()
        for entry in entries:
            q = entry.get("query", {})
            if "client" in q and "site" in q:
                try:
                    client_id = int(q["client"])
                    site_id = extract_leading_int(q["site"])
                    if client_id and site_id:
                        counter_key = (today, client_id, site_id)
                        self.site_hit_counters[counter_key] = self.site_hit_counters.get(counter_key, 0) + 1
                except (ValueError, TypeError) as e:
                    logger.debug(f"Skipping counter for malformed entry: {e}")

        # Check if we should flush counters
        current_time = time.time()
        if current_time - self.last_counter_flush > self.counter_flush_interval:
            await self.flush_site_hit_counters()

        # Phase 1: Gather impression IDs from Redis in one operation
        redis_keys = []
        entry_data = []
        messages_to_process = []

        with self.timer.time("prepare_batch"):
            for i, entry in enumerate(entries):
                q = entry["query"]
                raw_ip = entry.get("client_ip", "").split(",")[0].strip()

                try:
                    ip = format_ipv4_as_mapped_ipv6(raw_ip)
                except ValueError:
                    logger.warning(f"[SKIP] Invalid IP: {raw_ip}")
                    continue

                client_id = int(q["client"])
                site_id = extract_leading_int(q["site"])

                if site_id is None:
                    logger.warning(f"Invalid site ID: {q['site']} — skipping")
                    continue

                try:
                    if entry.get("timestamp"):
                        timestmp = datetime.fromisoformat(entry.get("timestamp"))
                        entry_ts = timestmp.timestamp()
                    else:
                        timestmp = datetime.now()
                        entry_ts = timestmp.timestamp()

                    # Check if this webhit is too recent - stop processing if so
                    if entry_ts > latest_imp_ts:
                        logger.info(f"Stopping batch processing at index {i}/{len(entries)} - "
                                    f"reached webhit newer than latest impression "
                                    f"({datetime.fromtimestamp(entry_ts).isoformat()} > "
                                    f"{datetime.fromtimestamp(float(latest_imp_ts)).isoformat() if latest_imp_ts else None})")
                        break
                except Exception:
                    timestmp = datetime.now()

                redis_keys.append(f"imp:{client_id}:{ip}")
                entry_data.append((entry, client_id, ip, site_id, timestmp))
                messages_to_process.append(i)

        # If no valid entries to process
        if not entry_data:
            return 0

        # Phase 2: Efficient Redis lookups with pipelining
        with self.timer.time("redis_lookup"):
            pipe = self.redis.pipeline()
            for key in redis_keys:
                pipe.get(key)
            impression_results = await pipe.execute()

        # Phase 3: Process entries with Redis ID generation
        with self.timer.time("processing"):
            processed_count = 0
            for (entry, client_id, ip, site_id, timestmp), redis_imp_id in zip(entry_data, impression_results):
                result = await self._process_single_webhit_impl_redis(client_id, ip, site_id, timestmp, redis_imp_id)
                if result:
                    processed_count += 1

        logger.info(f"Processed {processed_count}/{len(entry_data)} webhits, skipped rest due to timestamp cutoff")
        return processed_count

    async def _process_single_webhit_impl_redis(self, client_id, ip, site_id, timestmp, redis_imp_id):
        """Process a single webhit with Redis ID generation and optimized operations."""
        impression_id = int(redis_imp_id) if redis_imp_id else None

        # If no impression found, skip this webhit
        if not impression_id:
            return False

        # Deduplication check via Redis
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
        seen_sites = await self.redis.smembers(dedupe_key)

        if str(site_id) in seen_sites:
            logger.debug(f"Redis deduplication failed for {client_id=} {ip=} {site_id=}")
            return False

        # Get webhit ID from Redis
        webhit_id = await self.redis.incr("global:next_webhit_id")

        # Update Redis deduplication
        pipe = self.redis.pipeline()
        pipe.sadd(dedupe_key, site_id)
        pipe.expire(dedupe_key, settings.ONE_DAY)
        await pipe.execute()

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

        logger.debug(f"Processed webhit {webhit_id} for impression {impression_id}, client {client_id}, site {site_id}")
        return True

    async def flush_site_hit_counters(self):
        """Flush site hit counters using SQLAlchemy's MySQL dialect"""
        if not self.site_hit_counters:
            return

        current_counters = self.site_hit_counters.copy()
        self.site_hit_counters = {}
        self.last_counter_flush = time.time()

        try:
            async with self.db.async_session_scope('TVFACTORY') as session:
                for (date, client_id, site_id), hits in current_counters.items():
                    # MySQL-specific insert with on duplicate key update
                    insert_stmt = mysql_insert(TargetLastSiteResponse).values(
                        date=date,
                        client_id=client_id,
                        sitetag_id=site_id,
                        hits=hits
                    )

                    # The elegant part - on duplicate key update
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        hits=TargetLastSiteResponse.hits + hits
                    )

                    await session.execute(upsert_stmt)

                await session.commit()
                logger.info(f"Site activity: flushed {len(current_counters)} counters to database")
        except Exception as e:
            logger.error(f"Error flushing site activity counters: {e}")
            # Recover gracefully
            for key, count in current_counters.items():
                self.site_hit_counters[key] = self.site_hit_counters.get(key, 0) + count
