# services/impression_consumer.py

import asyncio
import ipaddress
import json
from datetime import datetime

from sqlalchemy import insert, func

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from models.impression import Impression
from utils.ip import format_ipv4_as_mapped_ipv6
from utils.timer import StepTimer

MAX_MESSAGES = 5000


class ImpressionConsumerService(BaseAsyncFactory):
    """
    Consumes impression messages from RabbitMQ and inserts them into the database.

    Side effects:
    - Creates a Redis key for impression deduplication.
    - Stores the impression ID by IP and client for webhit correlation.

    Flow:
    - Receives valid impression from queue.
    - Extracts key fields, formats IP.
    - Inserts into MariaDB.
    - Sets Redis keys:
        - imp:{client}:{ip} = impression_id (1 week)
        - dedupe:webhit:{client}:{ip} = delete (reset webhit tracking)
    """

    def __init__(self, run_once=False):
        super().__init__(require_redis=True, require_db=True, require_rabbit=True)
        self.queue_name = "raw_impressions_queue"
        self.timer = StepTimer()
        self.run_once = run_once
        logger.info("ImpressionConsumerService initialized")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        if self.rabbit:
            # Ensure our queues exist
            await self.rabbit.declare_queue(self.queue_name, durable=True)
            await self.rabbit.declare_queue("resolved_impressions_queue", durable=True)
        logger.info("ImpressionConsumerService setup complete")

    async def process_batch(self, batch_size=MAX_MESSAGES, timeout=30.0):
        """
        Processes a single batch of impression messages with sophisticated optimization.

        Args:
            batch_size: Maximum number of messages to process
            timeout: Maximum time to spend processing in seconds

        Returns:
            int: Number of messages processed
        """
        logger.info(f"Processing batch (max size: {batch_size})")
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
                            if self._is_valid_impression(payload):
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
            await self._handle_impression_batch(batch)

        # Acknowledge all messages after successful processing
        with self.timer.time("acknowledgment"):
            for msg in messages:
                await msg.ack()

        logger.info(f"Processed {len(batch)} impression messages")
        return len(batch)

    # Override _processing_loop to implement run_once behavior
    # For ImpressionConsumerService
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
    def _is_valid_impression(entry: dict) -> bool:
        """
        Verifies that the impression entry has all required query params.
        """
        q = entry.get("query", {})
        return all(k in q for k in ("client", "booking", "creative"))

    async def _handle_impression_batch(self, entries):
        """Process impressions with optimal database, Redis, and message queue operations."""
        values = []
        redis_updates = []
        publish_payloads = []

        for entry in entries:
            q = entry["query"]
            raw_ip = entry.get("client_ip", "").split(",")[0].strip()

            try:
                ipaddress.ip_address(raw_ip)
                ip = format_ipv4_as_mapped_ipv6(raw_ip)
            except ValueError:
                logger.warning(f"[SKIP] Invalid IP: {raw_ip}")
                continue

            timestmp = datetime.fromisoformat(entry.get("timestamp")) if entry.get("timestamp") else func.now()

            client_id = int(q["client"])
            booking_id = int(q["booking"])
            creative_id = int(q["creative"])

            values.append({
                "timestmp": timestmp,
                "client_id": client_id,
                "booking_id": booking_id,
                "creative_id": creative_id,
                "ipaddress": ip,
                "useragent": entry.get("user_agent", "")
            })

            redis_updates.append((client_id, ip))
            publish_payloads.append({
                "timestamp": entry.get("timestamp"),
                "client_id": client_id,
                "booking_id": booking_id,
                "creative_id": creative_id,
                "ipaddress": ip
            })

        # Insert into database
        async with self.db.async_session_scope('TVFACTORY') as session:
            stmt = insert(Impression).values(values).returning(Impression.id)
            result = await session.execute(stmt)
            ids = result.scalars().all()

        # Update Redis with impression ID for correlation and dedupe
        pipe = self.redis.pipeline()
        for (client_id, ip), new_id in zip(redis_updates, ids):
            redis_key = f"imp:{client_id}:{ip}"
            dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
            pipe.setex(redis_key, settings.ONE_WEEK, new_id)
            pipe.delete(dedupe_key)

        await pipe.execute()

        # Publish resolved impression message to RabbitMQ
        for msg, impression_id in zip(publish_payloads, ids):
            msg["id"] = impression_id
            await self.rabbit.publish(
                exchange="",
                routing_key="resolved_impressions_queue",
                message=msg
            )
