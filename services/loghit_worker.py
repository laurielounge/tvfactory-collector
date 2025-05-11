# services/loghit_worker.py
import asyncio

from core.logger import logger
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from services.loghit_processor import process_log_payload

LOG_QUEUE = "loghit_queue"
BATCH_SIZE = 1000

class LoghitWorkerService:
    def __init__(self, redis_client, rabbitmq_client):
        self.redis = redis_client
        self.rabbitmq = rabbitmq_client

    @classmethod
    async def create(cls):
        redis_client = await get_redis_client()
        rabbitmq_client = AsyncRabbitMQClient()
        await rabbitmq_client.connect()
        return cls(redis_client, rabbitmq_client)

    async def start(self, batch_size=BATCH_SIZE, interval_seconds=1, run_once=False):
        logger.info("LoghitWorkerService started.")
        await self.rabbitmq.connect()

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
                "impression": "impressions_queue",
                "webhit": "webhits_queue"
            }.get(category)

            if routing_key:
                await self.rabbitmq.publish("", routing_key, payload)
                logger.debug(f"Published to {routing_key}: {payload['host']}:{payload['line_num']}")
            count += 1
        return count
