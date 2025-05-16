# services/loghit_worker.py
import asyncio

from core.logger import logger
from infrastructure.circuit_breaker import RedisHealthChecker, RabbitMQHealthChecker, HealthCheckRegistry
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from services.loghit_processor import process_log_payload
from utils.async_factory import BaseAsyncFactory

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

    def __init__(self, redis_client, rabbitmq_client):
        self.redis = None
        self.rabbitmq = None

    async def async_setup(self):
        self.redis = await get_redis_client()
        self.rabbitmq = AsyncRabbitMQClient()
        await self.rabbitmq.connect()

    async def start(self, batch_size=BATCH_SIZE, interval_seconds=15, run_once=False):
        logger.info("LoghitWorkerService pre-start.")
        await self.rabbitmq.connect()
        registry = HealthCheckRegistry(
            RedisHealthChecker(self.redis),
            RabbitMQHealthChecker(self.rabbitmq),
        )
        await registry.assert_healthy_or_exit()
        logger.info("LoghitWorkerService started.")

        while True:
            count = await self._process_batch(batch_size)
            if run_once or count == 0:
                break
            await asyncio.sleep(interval_seconds)

        await self.rabbitmq.close()

    async def _process_batch(self, batch_size):
        count = 0
        for _ in range(batch_size):
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
                await self.rabbitmq.publish("", routing_key, payload)
                logger.debug(f"Published to {routing_key}: {payload['host']}:{payload['line_num']}")
            count += 1
        return count
