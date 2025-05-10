# services/impression_consumer.py

import asyncio
import json
from datetime import datetime

from sqlalchemy import insert, func

from config.config import settings
from core.logger import logger
from infrastructure.database import get_db_manager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from models.impression import Impression
from utils.ip import format_ipv4_as_mapped_ipv6
from utils.timer import StepTimer


class ImpressionConsumerService:
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

    def __init__(self, redis_client, rabbitmq):
        self.db = get_db_manager()
        self.redis = redis_client
        self.rabbitmq = rabbitmq
        self.queue_name = "impressions_queue"
        self.timer = StepTimer()

    @classmethod
    async def create(cls):
        redis_client = await get_redis_client()
        rabbitmq = AsyncRabbitMQClient()
        await rabbitmq.connect()
        return cls(redis_client, rabbitmq)

    async def start(self, run_once=False, max_messages=1000):
        logger.info("ImpressionConsumerService started")
        await self.rabbitmq.connect()
        if run_once:
            await self.run_once(max_messages=max_messages)
        else:
            await self.rabbitmq.consume(self.queue_name, self.process_message)
            return await asyncio.Future()  # Keeps service alive

    @staticmethod
    def _is_valid_impression(entry: dict) -> bool:
        """
        Verifies that the impression entry has all required query params.
        """
        q = entry.get("query", {})
        return all(k in q for k in ("client", "booking", "creative"))

    async def process_message(self, msg):
        """
        Callback for continuous queue consumption mode.
        Processes one message at a time.
        """
        body = msg.body if hasattr(msg, "body") else msg
        entry = json.loads(body)
        if self._is_valid_impression(entry):
            await self._handle_impression_batch([entry])
        await msg.ack()

    async def run_once(self, max_messages=1000):
        """
        One-shot processor for a batch of impressions from the queue.
        Pulls up to `max_messages` and processes them as a batch.
        """
        logger.info("ImpressionConsumerService one-shot mode started")
        count = 0
        batch = []
        messages = []

        for _ in range(max_messages):
            msg = await self.rabbitmq.get_message(self.queue_name)
            if not msg:
                break
            try:
                body = msg.body if hasattr(msg, "body") else msg
                payload = json.loads(body)
                if self._is_valid_impression(payload):
                    batch.append(payload)
                    messages.append(msg)
                    count += 1
                else:
                    logger.debug("Skipping incomplete impression.")
                    await msg.ack()
            except Exception as inner:
                logger.exception(f"[MESSAGE ERROR] Failed to parse message: {inner}")
                await msg.ack()

        if batch:
            await self._handle_impression_batch(batch)

        for msg in messages:
            await msg.ack()

        logger.info(f"Processed {count} impression messages.")

    async def _handle_impression_batch(self, entries: list[dict]):
        """
        Inserts a batch of impressions into the DB, and updates Redis accordingly:
        - Stores Redis index for future webhit lookup
        - Clears site dedupe cache for new impressions
        """
        values = []
        redis_updates = []

        for entry in entries:
            q = entry["query"]
            raw_ip = entry.get("client_ip", "").split(",")[0].strip()
            ip = format_ipv4_as_mapped_ipv6(raw_ip)
            ts_raw = entry.get("timestamp")

            try:
                timestmp = datetime.fromisoformat(ts_raw)
            except Exception:
                timestmp = func.now()

            client_id = int(q["client"])
            booking_id = int(q["booking"])
            creative_id = int(q["creative"])
            useragent = entry.get("user_agent", "")

            values.append({
                "timestmp": timestmp,
                "client_id": client_id,
                "booking_id": booking_id,
                "creative_id": creative_id,
                "ipaddress": ip,
                "useragent": useragent,
            })

            redis_updates.append((client_id, ip))

        with self.timer.time("prepare_and_insert"):
            with self.db.session_scope('TVFACTORY') as session:
                stmt = insert(Impression).values(values).returning(Impression.id)
                results = session.execute(stmt).scalars().all()

        with self.timer.time("redis_update"):
            for (client_id, ip), new_id in zip(redis_updates, results):
                redis_key = f"imp:{client_id}:{ip}"
                dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
                await self.redis.setex(redis_key, settings.ONE_WEEK, new_id)
                await self.redis.delete(dedupe_key)
                logger.debug(f"[IMPRESSION INSERTED] client={client_id} ip={ip} id={new_id}")

        self.timer.tick()
