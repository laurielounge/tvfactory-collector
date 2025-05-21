# services/impression_consumer.py

import asyncio
import json
from datetime import datetime

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from models.impression import Impression
from utils.redis_bootstrap import sync_redis_id_counter
from utils.timer import StepTimer

MAX_MESSAGES = 5000


class ImpressionConsumerService(BaseAsyncFactory):
    """
    Consumes impression messages from RabbitMQ and inserts them into the database.

    Side effects:
    - Creates a Redis key for webhit deduplication.
    - Stores the impression ID by IP and client for webhit correlation.

    Flow:
    - Receives valid impression from queue.
    - Extracts key fields, formats IP.
    - Only validation is, is it a valid impression, but it could also drop "banned client" data.
    - Inserts into resolved_impressions_queue.
    - Sets Redis keys:
        - imp:{client}:{ip} = impression_id (1 week)
        - dedupe:webhit:{client}:{ip} = delete (reset webhit tracking)
    - Does save to database "edge_data".
    - All publish to database is done by the data warehouse.
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
        await sync_redis_id_counter(
            redis=self.redis,
            db=self.db,
            redis_key="global:next_impression_id",
            model=Impression,
            id_column=Impression.id
        )
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
        """Process impressions with Redis ID generation and optimal message handling."""
        redis_updates = []
        publish_payloads = []
        latest_timestamp = 0

        # Get IDs in one batch operation for efficiency
        pipe = self.redis.pipeline()
        for _ in range(len(entries)):
            pipe.incr("global:next_impression_id")
        impression_ids = await pipe.execute()

        for entry, impression_id in zip(entries, impression_ids):
            q = entry["query"]

            # Parse timestamp or use current time
            if entry.get("timestamp"):
                timestmp = datetime.fromisoformat(entry.get("timestamp"))
                timestamp_float = timestmp.timestamp()
                # Track the latest timestamp for chronological processing
                latest_timestamp = max(latest_timestamp, timestamp_float)
            else:
                timestmp = datetime.now()
                timestamp_float = timestmp.timestamp()
                latest_timestamp = max(latest_timestamp, timestamp_float)

            client_id = int(q["client"])
            booking_id = int(q["booking"])
            creative_id = int(q["creative"])
            ip = entry["ipaddress"]
            redis_updates.append((client_id, ip, impression_id))

            # Prepare payload for RabbitMQ with Redis-generated ID
            publish_payloads.append({
                "id": impression_id,
                "timestmp": entry.get("timestamp") or timestmp.isoformat(),
                "client_id": client_id,
                "booking_id": booking_id,
                "creative_id": creative_id,
                "ipaddress": ip,
                "useragent": entry.get("user_agent", "")
            })

        # Execute Redis updates in a single pipeline operation
        pipe = self.redis.pipeline()

        # Update latest impression timestamp for webhit correlation
        if latest_timestamp > 0:
            pipe.set("latest_impression_timestamp", latest_timestamp)

        # Set all the correlation and dedupe keys
        for client_id, ip, impression_id in redis_updates:
            redis_key = f"imp:{client_id}:{ip}"
            dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
            pipe.setex(redis_key, settings.ONE_WEEK, impression_id)
            pipe.delete(dedupe_key)
        today_date = datetime.now().strftime("%Y-%m-%d")
        for entry, impression_id in zip(entries, impression_ids):
            q = entry["query"]
            client_id = int(q["client"])
            booking_id = int(q["booking"])

            pipe.zincrby(f"daily:impressions:customers:{today_date}", 1, client_id)
            pipe.zincrby(f"daily:impressions:bookings:{today_date}", 1, booking_id)

        await pipe.execute()

        # Publish to RabbitMQ
        for payload in publish_payloads:
            await self.rabbit.publish(
                exchange="",
                routing_key="resolved_impressions_queue",
                message=payload
            )

        logger.info(
            f"Processed {len(publish_payloads)} impressions with Redis IDs, latest timestamp: {latest_timestamp}")
