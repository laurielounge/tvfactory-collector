# services/loghit_worker.py
import asyncio

from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from services.loghit_processor import process_log_payload

LOG_QUEUE = "loghit_queue"
BATCH_SIZE = 5000


class LoghitWorkerService(BaseAsyncFactory):
    """
    Processes raw log payloads from Redis and dispatches them to RabbitMQ based on type.

    Responsibilities:
    - RPOP from Redis list `loghit_queue`
    - Parse and classify each log entry as either:
        - impression → publish to `raw_impressions_queue`
        - webhit     → publish to `raw_webhits_queue`
    - Drops malformed or unclassifiable entries
    - Supports batch processing and optional interval looping
    - Does NOT extract the ipaddress from the log line

    External Systems:
    - ✅ Reads from Redis (`loghit_queue`)
    - ✅ Writes to RabbitMQ (`raw_impressions_queue`, `raw_webhits_queue`)

    Downstream:
    - ImpressionConsumerService and WebhitConsumerService, which consume from those raw queues
    """

    def __init__(self, run_once=False):
        super().__init__(require_redis=True, require_rabbit=True)
        self.run_once = run_once
        logger.info("LoghitWorkerService initialized")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        # Ensure queues are declared
        if self.rabbit:
            await self.rabbit.declare_standard_queues()
        logger.info("LoghitWorkerService setup complete")

    async def process_batch(self, batch_size: int = BATCH_SIZE, timeout: float = 30.0) -> int:
        """
        Process a batch of log entries from Redis and publish to appropriate RabbitMQ queues.

        Args:
            batch_size: Maximum number of log entries to process
            timeout: Maximum processing time (currently unused)

        Returns:
            int: Number of log entries successfully processed
        """
        if not self.redis or not self.rabbit:
            logger.error("Cannot process batch: Redis or RabbitMQ connection missing")
            return 0

        count = 0
        start_time = asyncio.get_event_loop().time()

        for _ in range(batch_size):
            # Check timeout
            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.warning(f"Batch processing timed out after {count} entries")
                break

            # Get next item from Redis
            raw = await self.redis.rpop(LOG_QUEUE)
            if not raw:
                break

            try:
                result = process_log_payload(raw)
            except Exception as e:
                logger.warning(f"Failed to process log line: {e}")
                continue

            if not result:
                continue

            category, payload = result
            routing_key = {
                "impression": "raw_impressions_queue",
                "webhit": "raw_webhits_queue"
            }.get(category)

            if routing_key:
                await self.rabbit.publish("", routing_key, payload)
                logger.debug(f"Published to {routing_key}: {payload['host']}:{payload['line_num']}")
            count += 1

            # Early exit if run_once and we've processed at least one item
            if self.run_once and count > 0:
                break

        logger.info(f"Processed {count} log entries")
        return count

    # Override _processing_loop to implement run_once behavior
    async def _processing_loop(self, interval_seconds):
        """Processing loop with optional run-once behavior"""
        while self._running:
            try:
                items_processed = await self.process_batch()

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
