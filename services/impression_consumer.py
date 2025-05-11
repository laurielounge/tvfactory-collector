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
from models.impression import Impression, FinishedImpression
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
        """Efficiently processes messages in proper batches to minimize network overhead."""
        logger.info("ImpressionConsumerService processing batch")
        batch = []
        messages = []

        # The critical optimization: retrieve messages in bulk
        with self.timer.time("message_retrieval"):
            # Create connection and channel if needed (once, not per message)
            if not self.rabbitmq.channel:
                await self.rabbitmq.connect()

            # Get queue reference once
            queue = await self.rabbitmq.channel.declare_queue(
                self.queue_name, passive=True, durable=True
            )

            # Set prefetch to improve throughput
            await self.rabbitmq.channel.set_qos(prefetch_count=max_messages)

            # Batch collect with limited iterator lifetime
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        payload = json.loads(message.body)
                        if self._is_valid_impression(payload):
                            batch.append(payload)
                            messages.append(message)
                            if len(batch) >= max_messages:
                                break
                        else:
                            await message.ack()
                    except Exception as e:
                        logger.exception(f"Message parsing error: {e}")
                        await message.ack()

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

    async def _handle_impression_batch(self, entries):
        """Process impressions with optimal database and Redis operations."""
        values = []
        redis_updates = []

        # Prepare data arrays
        for entry in entries:
            q = entry["query"]
            raw_ip = entry.get("client_ip", "").split(",")[0].strip()
            ip = format_ipv4_as_mapped_ipv6(raw_ip)

            # Simplified timestamp handling
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

        # Database operation
        with self.db.session_scope('TVFACTORY') as session:
            stmt = insert(Impression).values(values).returning(Impression.id)
            results = session.execute(stmt).scalars().all()

        # Redis pipeline operation
        pipe = self.redis.pipeline()
        for (client_id, ip), new_id in zip(redis_updates, results):
            redis_key = f"imp:{client_id}:{ip}"
            dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
            pipe.setex(redis_key, settings.ONE_WEEK, new_id)
            pipe.delete(dedupe_key)

        await pipe.execute()
