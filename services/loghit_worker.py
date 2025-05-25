# services/loghit_worker.py
import asyncio
import json
import logging
from datetime import datetime

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from services.vector_processor import process_vector_payload

LOG_QUEUE = "loghit_queue"
BATCH_SIZE = 10000



class LoghitWorkerService(BaseAsyncFactory):
    """
    Processes raw log payloads from RabbitMQ and dispatches them to downstream queues.

    Responsibilities:
    - Consumes from RabbitMQ queue `loghit_queue`
    - Parse and classify each log entry as either:
        - impression → publish to `raw_impressions_queue`
        - webhit     → publish to `raw_webhits_queue`
    - Drops malformed or unclassifiable entries
    - Supports batch processing and optional interval looping

    External Systems:
    - ✅ Reads from RabbitMQ (`loghit_queue`)
    - ✅ Writes to RabbitMQ (`raw_impressions_queue`, `raw_webhits_queue`)

    Downstream:
    - ImpressionConsumerService and WebhitConsumerService, which consume from those raw queues
    """

    def __init__(self, run_once=False):

        super().__init__(require_redis=True, require_rabbit=True)
        self.queue_name = LOG_QUEUE
        self.run_once = run_once
        logger.info("LoghitWorkerService initialized")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        # Ensure queues are declared
        if self.rabbit:
            await self.rabbit.declare_queue(self.queue_name, durable=True)
            await self.rabbit.declare_standard_queues()
        logger.info("LoghitWorkerService setup complete")

    # In loghit_worker.py - modify process_batch
    async def process_batch(self, batch_size: int = BATCH_SIZE, timeout: float = 30.0) -> int:
        """
        Process a batch of log entries from RabbitMQ with detailed diagnostic logging.
        """
        if not self.rabbit:
            logger.error("Cannot process batch: RabbitMQ connection missing")
            return 0

        # Override batch size for intense debugging
        logger.info(f"Retrieving batch of {batch_size} messages")
        messages = await self.rabbit.get_batch(self.queue_name, batch_size)

        if not messages:
            return 0

        logger.debug(f"Retrieved {len(messages)} messages for debugging")

        # Process each message with intense logging
        count = 0
        for message in messages:
            try:
                # Parse message directly without conversion
                payload = json.loads(message.body)
                logger.debug(f"Received {payload}")
                # Count raw message per edge server by hostname
                try:
                    today = datetime.now(settings.TIME_ZONE).strftime("%Y-%m-%d")
                    hostname = payload.get("hostname", payload.get("host", "unknown"))
                    key = f"daily:edgeserver:{hostname}:{today}"
                    await self.redis.incr(key)
                except Exception as e:
                    logger.warning(f"[EDGECOUNTER FAIL] Could not increment Redis counter for host={hostname}: {e}")

                result = process_vector_payload(payload)

                if result:
                    category, entry = result
                    routing_key = {
                        "impression": "raw_impressions_queue",
                        "webhit": "raw_webhits_queue"
                    }.get(category)

                    if routing_key:
                        await self.rabbit.publish("", routing_key, entry)
                        count += 1

                # Always acknowledge
                await message.ack()

            except Exception as e:
                logger.warning(f"Failed to process log message: {str(e)}", exc_info=True)
                await message.ack()

        return count

    # Override _processing_loop to implement run_once behavior
    async def _processing_loop(self, interval_seconds, batch_size=None, **kwargs):
        """Processing loop with optional run-once behavior"""
        while self._running:
            try:
                items_processed = await self.process_batch(batch_size=batch_size)

                # If run_once is set and we've processed items, exit the loop
                if self.run_once and items_processed > 0:
                    logger.info("Run-once mode: exiting after successful batch")
                    break

                log_level = logging.INFO if items_processed > 0 else logging.DEBUG
                logger.log(log_level, f"Processed {items_processed} items")

                # Only sleep if we're continuing
                if not self.run_once:
                    await asyncio.sleep(interval_seconds)

            except asyncio.CancelledError:
                logger.info(f"{self.__class__.__name__} processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                await asyncio.sleep(interval_seconds / 2)
