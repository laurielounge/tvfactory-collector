# services/redis_collector_service.py

import asyncio
import json

from core.logger import logger
from utils.redis_client import get_redis_client
from utils.timer import StepTimer

timer = StepTimer()
BATCH_SIZE = 100


class RedisCollectorService:
    """Collects impression and webhit data from Redis and forwards it to RabbitMQ."""

    def __init__(self, rabbitmq_client):
        self.redis = get_redis_client()
        self.rabbitmq = rabbitmq_client
        self.running = False

    async def start(self, interval_seconds: int = 30):
        """Begin the collection cycle from Redis."""
        if self.running:
            logger.warning("RedisCollectorService is already running")
            return

        self.running = True
        logger.info("RedisCollectorService started")
        try:
            while self.running:
                await self.collect_and_queue()
                logger.info(f"Redis collection cycle complete. Sleeping for {interval_seconds} seconds")
                await asyncio.sleep(interval_seconds)
        finally:
            self.running = False

    async def stop(self):
        """Stop the collection cycle gracefully."""
        logger.info("Stopping RedisCollectorService")
        self.running = False

    async def collect_and_queue(self):
        """Pulls records from Redis and publishes to RabbitMQ."""
        await asyncio.gather(
            self._process_redis_queue("impression_queue", "impressions_queue"),
            self._process_redis_queue("webhit_queue", "webhits_queue")
        )

    async def _process_redis_queue(self, redis_queue: str, rabbit_queue: str):
        """Pull from a Redis queue and forward to the appropriate RabbitMQ routing key."""
        total_processed = 0

        with timer.time(f"{redis_queue}_dequeue"):
            for _ in range(BATCH_SIZE):
                raw = self.redis.rpop(redis_queue)
                if not raw:
                    break

                try:
                    with timer.time(f"{redis_queue}_parse"):
                        message = json.loads(raw)

                    with timer.time(f"{redis_queue}_publish"):
                        await self.rabbitmq.publish(
                            exchange='',
                            routing_key=rabbit_queue,
                            message=message
                        )
                    total_processed += 1
                except Exception as e:
                    logger.error(f"[REDIS SYNC ERROR] Failed to publish from {redis_queue}: {e}")

        if total_processed:
            logger.info(f"Processed {total_processed} items from {redis_queue}")
        timer.tick()
