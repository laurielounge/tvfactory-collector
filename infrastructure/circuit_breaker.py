# infrastructure/circuit_breaker.py
import asyncio
import time

from sqlalchemy import text

from core.logger import logger


class HealthCheckRegistry:
    def __init__(self, *checkers):
        self.checkers = checkers
        self.last_failures = {}  # Track when each service last failed
        self.recovery_attempts = {}  # Count recovery attempts per service

    async def is_healthy(self) -> bool:
        """Basic health check for all registered services."""
        results = await asyncio.gather(*(chk.is_alive() for chk in self.checkers))
        unhealthy_services = []

        for chk, ok in zip(self.checkers, results):
            service_name = chk.__class__.__name__
            if not ok:
                unhealthy_services.append(service_name)
                logger.error(f"[HEALTH] {service_name} reported unhealthy.")

                # Track failure
                if service_name not in self.last_failures:
                    self.last_failures[service_name] = time.time()
                    self.recovery_attempts[service_name] = 0
                else:
                    self.recovery_attempts[service_name] += 1
            elif service_name in self.last_failures:
                # Service recovered
                recovery_time = time.time() - self.last_failures[service_name]
                attempts = self.recovery_attempts[service_name]
                logger.info(f"[HEALTH] {service_name} recovered after {recovery_time:.1f}s and {attempts} attempts")
                del self.last_failures[service_name]
                del self.recovery_attempts[service_name]

        return len(unhealthy_services) == 0

    async def assert_healthy_or_exit(self):
        """Validates health and exits if any service is unhealthy (strict mode)."""
        if not await self.is_healthy():
            logger.critical("🚨 One or more critical services are down. Shutting down.")
            import sys
            sys.exit(1)

    async def check_with_backoff(self, max_wait=60, initial_delay=1):
        """
        Checks health with exponential backoff for recovery.

        Args:
            max_wait: Maximum seconds to wait between retries
            initial_delay: Initial delay in seconds

        Returns:
            bool: True if healthy, False if still unhealthy after backoff
        """
        if await self.is_healthy():
            return True

        # Services are unhealthy, determine appropriate backoff
        max_attempts = max(self.recovery_attempts.values()) if self.recovery_attempts else 0
        delay = min(initial_delay * (2 ** max_attempts), max_wait)

        logger.warning(f"Services unhealthy. Backing off for {delay:.1f}s before retry (attempt {max_attempts + 1})")
        await asyncio.sleep(delay)

        # Check again after backoff
        return await self.is_healthy()


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
