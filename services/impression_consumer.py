# services/impression_consumer.py

import asyncio
import ipaddress
import json
from datetime import datetime

from sqlalchemy import insert, func

from config.config import settings
from core.logger import logger
from infrastructure.circuit_breaker import RedisHealthChecker, RabbitMQHealthChecker, MariaDBHealthChecker, \
    HealthCheckRegistry
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from models.impression import Impression
from utils.async_factory import BaseAsyncFactory
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

    def __init__(self):
        self.db = None
        self.redis = None
        self.rabbitmq = None
        self.queue_name = "raw_impressions_queue"
        self.timer = StepTimer()

    async def async_setup(self):
        self.db = AsyncDatabaseManager()
        await self.db.initialize()
        self.rabbitmq = AsyncRabbitMQClient()
        await self.rabbitmq.connect()
        self.redis = await get_redis_client()

    async def start(self, run_once=False, max_messages=MAX_MESSAGES):
        logger.info("ImpressionConsumerService started")
        await self.rabbitmq.connect()
        registry = HealthCheckRegistry(
            RedisHealthChecker(self.redis),
            RabbitMQHealthChecker(self.rabbitmq),
            MariaDBHealthChecker(self.db)
        )

        await registry.assert_healthy_or_exit()
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

    async def run_once(self, max_messages=MAX_MESSAGES):
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

            # Batch collect with graceful timeout handling
            try:
                async with queue.iterator(timeout=2.0) as queue_iter:
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
            await self.rabbitmq.publish(
                exchange="",
                routing_key="resolved_impressions_queue",
                message=msg
            )
