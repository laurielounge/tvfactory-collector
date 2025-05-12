# infrastructure/circuit_breaker.py
import asyncio

from sqlalchemy import text

from core.logger import logger


class HealthCheckRegistry:
    def __init__(self, *checkers):
        self.checkers = checkers

    async def is_healthy(self) -> bool:
        results = await asyncio.gather(*(chk.is_alive() for chk in self.checkers))
        if not all(results):
            for chk, ok in zip(self.checkers, results):
                if not ok:
                    logger.error(f"[HEALTH] {chk.__class__.__name__} reported unhealthy.")
            return False
        return True

    async def assert_healthy_or_exit(self):
        if not await self.is_healthy():
            logger.critical("🚨 One or more critical services are down. Shutting down.")
            import sys
            sys.exit(1)


class RabbitMQHealthChecker:
    def __init__(self, client):
        self.client = client

    async def is_alive(self) -> bool:
        try:
            await self.client.connect()
            return not self.client.connection.is_closed
        except Exception as e:
            logger.warning(f"RabbitMQ health check failed: {e}")
            return False


class RedisHealthChecker:
    def __init__(self, redis_client):
        self.redis = redis_client

    async def is_alive(self) -> bool:
        try:
            await self.redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False


class MariaDBHealthChecker:
    def __init__(self, db_manager, db_key="TVFACTORY"):
        self.db_manager = db_manager
        self.db_key = db_key

    async def is_alive(self) -> bool:
        try:
            async with self.db_manager.async_session_scope(self.db_key) as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.warning(f"MariaDB health check failed: {e}")
            return False
