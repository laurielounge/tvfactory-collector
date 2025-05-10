# services/webhit_consumer.py

import asyncio
import json
from datetime import datetime

from sqlalchemy import func, select, and_, text, insert

from config.config import settings
from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client
from models.impression import FinishedImpression as Impression
from models.webhit import WebHit
from utils.ip import format_ipv4_as_mapped_ipv6
from utils.timer import StepTimer


class WebhitConsumerService:
    """
    Consumes webhit messages from RabbitMQ, checks against Redis for a matching impression,
    deduplicates by site, and inserts into the database if valid.

    Flow:
    - Looks up impression ID from Redis (using client_id + IP).
    - If not found, optionally falls back to DB (7-day lookback).
    - Checks if this site_id has already been seen for this impression via Redis and DB.
    - If not a duplicate, inserts a new WebHit.
    """

    def __init__(self, redis_client, rabbitmq):

        self.db = AsyncDatabaseManager()
        self.rabbitmq = rabbitmq
        self.queue_name = "webhits_queue"
        self.timer = StepTimer()
        self.redis = redis_client

    @classmethod
    async def create(cls):
        redis_client = await get_redis_client()
        rabbitmq = AsyncRabbitMQClient()
        await rabbitmq.connect()
        return cls(redis_client, rabbitmq)

    async def start(self, run_once=True, max_messages=1000):
        logger.info("WebhitConsumerService started")
        await self.db.initialize()
        await self.rabbitmq.connect()
        try:
            if run_once:
                await self.run_once(max_messages=max_messages)
            else:
                await self.rabbitmq.consume(self.queue_name, self.process_message)
                return await asyncio.Future()
        finally:
            await self.db.close()
            await self.rabbitmq.close()
            logger.info("Connections closed")

    async def run_once(self, max_messages=1000):
        logger.info("WebhitConsumerService one-shot mode started")
        count = 0
        batch = []
        messages = []

        for _ in range(max_messages):
            msg = await self.rabbitmq.get_message(self.queue_name)
            if not msg:
                break
            try:
                body = msg.body if hasattr(msg, "body") else msg
                entry = json.loads(body)
                if self._is_valid_webhit(entry):
                    batch.append(entry)
                    messages.append(msg)
                    count += 1
                else:
                    logger.debug("Skipping invalid webhit.")
                    await msg.ack()
            except Exception as e:
                logger.warning(f"[WEBHIT ERROR] Parse failed: {e}")
                await msg.ack()

        if batch:
            await self._handle_webhit_batch(batch)

        for msg in messages:
            await msg.ack()

        logger.info(f"Processed {count} webhit messages.")

    @staticmethod
    def _is_valid_webhit(entry: dict) -> bool:
        """
        Checks if the entry contains a valid webhit query:
        requires 'client' and 'site' keys.
        """
        q = entry.get("query", {})
        return all(k in q for k in ("client", "site"))

    async def _handle_webhit_batch(self, entries: list[dict]):
        """
        Processes a batch of valid webhit messages.
        Each webhit is validated, deduplicated, and possibly inserted into the DB.
        """
        async with self.db.async_session_scope('TVFACTORY') as db:
            for entry in entries:
                try:
                    await self._handle_single_webhit(entry, db)
                except Exception as e:
                    logger.warning(f"[WEBHIT ERROR] {e}")

    async def _handle_single_webhit(self, entry: dict, db):
        """
        Core webhit processing logic:
        - Looks up impression from Redis or DB.
        - Deduplicates by site_id in Redis and DB.
        - Inserts WebHit if not already seen.
        """
        q = entry["query"]
        ts_raw = entry.get("timestamp")
        try:
            timestmp = datetime.fromisoformat(ts_raw)
        except Exception:
            timestmp = func.now()

        raw_ip = entry.get("client_ip", "").split(",")[0].strip()
        ip = format_ipv4_as_mapped_ipv6(raw_ip)
        client_id = int(q["client"])
        site_id = int(q["site"])

        logger.debug(f"[WEBHIT] client={client_id} ip={ip} site={site_id}")

        redis_imp_id = await self.redis.get(f"imp:{client_id}:{ip}")
        impression_id = int(redis_imp_id) if redis_imp_id else None

        if not impression_id:
            try:
                result = await db.execute(
                    select(Impression.id).where(
                        and_(
                            Impression.client_id == client_id,
                            Impression.ipaddress == ip,
                            Impression.timestmp > func.now() - text("INTERVAL 7 DAY"),
                        )
                    ).order_by(Impression.timestmp.desc()).limit(1)
                )
                row = result.scalar_one_or_none()
                if not row:
                    logger.debug(f"[WEBHIT] No matching impression in DB for {client_id=} {ip=}")
                    return
                impression_id = row
            except Exception as e:
                logger.warning(f"[QUERY ERROR] {e}")
                return

            impression_id = row[0]

        # Redis deduplication
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}"
        seen_sites = await self.redis.smembers(dedupe_key)
        if str(site_id) in seen_sites:
            logger.debug(f"[WEBHIT REDIS DUPLICATE] site={site_id} already seen for {client_id=} {ip=}")
            return

        # DB deduplication
        result = await db.execute(
            select(WebHit.id).where(
                and_(
                    WebHit.impression_id == impression_id,
                    WebHit.site_id == site_id,
                    WebHit.timestmp > func.now() - text("INTERVAL 1 DAY"),
                )
            )
        )
        if result.scalar_one_or_none():
            logger.debug(f"[WEBHIT DB DUPLICATE] Already saved for {client_id=} {site_id=} {impression_id=}")
            return

        # Insert WebHit
        await db.execute(
            insert(WebHit).values(
                impression_id=impression_id,
                client_id=client_id,
                site_id=site_id,
                ipaddress=ip,
                timestmp=timestmp,
            )
        )
        await self.redis.sadd(dedupe_key, site_id)
        await self.redis.expire(dedupe_key, settings.ONE_DAY)

        logger.info(f"[WEBHIT SAVED] {client_id=} {site_id=} {ip=} {impression_id=}")
