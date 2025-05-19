# services/webhit_consumer.py

import asyncio
import json
import re
import time
from datetime import datetime, timedelta

from sqlalchemy import select, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from models.impression import Impression
from models.last_site_response import TargetLastSiteResponse
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
        """Process webhits chronologically, stopping at too-recent entries."""
        logger.info(f"Processing batch of webhit messages (max size: {batch_size})")

        # Fetch latest impression timestamp first
        latest_imp_ts = await self.redis.get("latest_impression_timestamp")
        if not latest_imp_ts:
            latest_imp_ts = 0
        else:
            latest_imp_ts = float(latest_imp_ts)

        logger.info(
            f"Latest impression timestamp: {datetime.fromtimestamp(float(latest_imp_ts)).isoformat() if latest_imp_ts else None}")

        # Check if we should flush counters
        today = datetime.now().date()
        current_time = time.time()
        if current_time - self.last_counter_flush > self.counter_flush_interval:
            await self.flush_site_hit_counters()

        # Process messages, stopping when we hit one that's too recent
        processed_count = 0

        # Create connection and channel
        if not self.rabbit.channel:
            await self.rabbit.connect()
        queue = await self.rabbit.channel.declare_queue(self.queue_name, passive=True, durable=True)
        await self.rabbit.channel.set_qos(prefetch_count=batch_size)

        try:
            async with queue.iterator(timeout=2.0) as queue_iter:
                async for message in queue_iter:
                    try:
                        # Parse message
                        payload = json.loads(message.body)

                        # Check basic validity
                        if not self._is_valid_webhit(payload):
                            await message.ack()  # Remove invalid message
                            continue

                        # Extract data from payload
                        q = payload.get("query", {})
                        client_id = None
                        site_id = None

                        try:
                            client_id = int(q["client"])
                            site_id = extract_leading_int(q["site"])

                            if site_id is None:
                                logger.warning(f"Invalid site ID: {q['site']} — skipping")
                                await message.ack()
                                continue
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid client ID or site ID")
                            await message.ack()
                            continue

                        # Check timestamp chronology
                        if payload.get("timestamp"):
                            try:
                                entry_ts = datetime.fromisoformat(payload.get("timestamp")).timestamp()
                            except ValueError:
                                entry_ts = datetime.now().timestamp()
                        else:
                            entry_ts = datetime.now().timestamp()

                        # If this webhit is newer than our latest impression, stop processing
                        if entry_ts > latest_imp_ts:
                            logger.info(f"Stopping batch - reached webhit newer than latest impression")
                            break  # Don't acknowledge - leave in queue for later

                        # Extract IP
                        ip = payload.get("ipaddress")

                        # Update site hit counter - do this ONCE when we know the entry is valid
                        counter_key = (today, client_id, site_id)
                        self.site_hit_counters[counter_key] = self.site_hit_counters.get(counter_key, 0) + 1

                        # Process the webhit
                        result = await self._process_webhit_find_impression(
                            client_id, ip, site_id, datetime.fromtimestamp(entry_ts))

                        if result:
                            processed_count += 1

                        # Acknowledge after processing
                        await message.ack()

                        # Check batch size limit
                        if processed_count >= batch_size:
                            break

                    except Exception as e:
                        logger.exception(f"Error processing message: {e}")
                        await message.ack()  # Remove problematic message

        except TimeoutError:
            logger.debug("Queue iterator timed out - this is normal when queue is empty")

        logger.info(f"Processed {processed_count} webhit messages")
        return processed_count

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

    @staticmethod
    def _is_valid_webhit(entry: dict) -> bool:
        """
        Checks if the entry contains a valid webhit query:
        requires 'client' and 'site' keys.
        """
        q = entry.get("query", {})
        logger.debug(f"[IS VALID WEBHIT] Query string was {q} and was {all(k in q for k in ('client', 'site') if q)}")
        return all(k in q for k in ("client", "site"))

    async def _process_webhit_find_impression(self, client_id, ip, site_id, timestmp) -> bool:
        """Determine if this is a deduplicated, valid webhit and process it."""

        # Redis keys
        imp_key = f"imp:{client_id}:{ip}"

        # --- Step 1: Fast-path Redis check ---
        impression_id = await self.redis.get(imp_key)

        if impression_id:
            impression_id = int(impression_id)
            dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
            seen_sites = await self.redis.smembers(dedupe_key)

            if str(site_id) in seen_sites:
                logger.debug(f"[DEDUPE REDIS] Site already seen: client={client_id}, ip={ip}, site_id={site_id}")
                return False

            logger.debug(f"[MATCH REDIS] client={client_id}, ip={ip}, imp_id={impression_id}, site_id={site_id}")

        else:
            # --- Step 2: Fallback to DB ---
            try:
                async with self.db.async_session_scope('TVFACTORY') as db:
                    seven_days_ago = timestmp - timedelta(days=7)

                    # Find latest impression
                    result = await db.execute(
                        select(Impression.id).where(
                            and_(
                                Impression.client_id == client_id,
                                Impression.ipaddress == ip,
                                Impression.timestmp > seven_days_ago
                            )
                        ).order_by(Impression.timestmp.desc()).limit(1)
                    )
                    impression_id = result.scalars().first()

                    if not impression_id:
                        logger.debug(f"[NO MATCH] No impression found in DB for client={client_id}, ip={ip}")
                        return False

                    logger.debug(f"[MATCH DB] client={client_id}, ip={ip}, imp_id={impression_id}")

                    # Check for DB dedupe
                    one_day_ago = timestmp - timedelta(days=1)
                    result = await db.execute(
                        select(WebHit.id).where(
                            and_(
                                WebHit.impression_id == impression_id,
                                WebHit.site_id == site_id,
                                WebHit.timestmp > one_day_ago
                            )
                        )
                    )
                    if result.scalars().first():
                        logger.debug(
                            f"[DEDUPE DB] Webhit already recorded for imp_id={impression_id}, site_id={site_id}")
                        return False
            except Exception as e:
                logger.warning(f"[DB ERROR] Failed during DB fallback: {e}")
                return False

        # --- Step 3: Accept and Record ---
        try:
            webhit_id = await self.redis.incr("global:next_webhit_id")

            # Update Redis dedupe list
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

            logger.debug(f"[ACCEPTED] Webhit {webhit_id} published for imp_id={impression_id}, site_id={site_id}")
            return True

        except Exception as e:
            logger.error(f"[FAIL PUBLISH] Could not publish webhit: {e}")
            return False

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
