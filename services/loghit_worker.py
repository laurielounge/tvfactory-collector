# services/loghit_worker.py
import asyncio

from core.logger import logger
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from services.loghit_processor import process_log_payload

LOG_QUEUE = "loghit_queue"


class LoghitWorkerService:
    def __init__(self):
        self.redis = get_redis_client()
        self.rabbitmq = AsyncRabbitMQClient()

    async def start(self, batch_size=100, interval_seconds=1, run_once=False):
        logger.info("LoghitWorkerService started.")
        await self.rabbitmq.connect()

        while True:
            await self._process_batch(batch_size)
            if run_once:
                break
            await asyncio.sleep(interval_seconds)

        await self.rabbitmq.close()

    async def _process_batch(self, batch_size):
        for _ in range(batch_size):
            raw = self.redis.rpop(LOG_QUEUE)
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
