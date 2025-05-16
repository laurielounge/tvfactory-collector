# infrastructure/async_factory.py
import asyncio
from abc import ABC, abstractmethod
from typing import TypeVar, Type

from core.logger import logger
from infrastructure.circuit_breaker import RedisHealthChecker, MariaDBHealthChecker, RabbitMQHealthChecker, \
    HealthCheckRegistry
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from infrastructure.redis_client import get_redis_client

T = TypeVar("T", bound="BaseAsyncFactory")


class BaseAsyncFactory(ABC):
    """
    Sophisticated base class for async service components with connection management,
    health checking, and standardized lifecycle methods.
    """

    def __init__(self, require_redis=False, require_db=False, require_rabbit=False):
        self.redis = None
        self.db = None
        self.rabbit = None
        self._requirements = {
            "redis": require_redis,
            "db": require_db,
            "rabbit": require_rabbit
        }
        self._registry = None
        self._running = False
        self._task = None
        logger.info(f"Initializing {self.__class__.__name__} with requirements: {self._requirements}")

    @classmethod
    async def create(cls: Type[T], **kwargs) -> T:
        """Factory method for creating and initializing services with elegant simplicity."""
        logger.info(f"Creating {cls.__name__} instance")
        instance = cls(**kwargs)
        await instance.async_setup()
        return instance

    async def __aenter__(self):
        """Context manager entry with automatic initialization."""
        await self.initialise()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with graceful shutdown."""
        await self.stop()

    async def async_setup(self):
        """Establishes required connections and health checks with sophisticated error handling."""
        checkers = []
        connection_failures = []

        # Redis connection
        if self._requirements["redis"]:
            try:
                logger.info(f"{self.__class__.__name__} initializing Redis connection")
                self.redis = await get_redis_client()
                if self.redis:
                    checkers.append(RedisHealthChecker(self.redis))
                    logger.debug("Redis connection established")
                else:
                    connection_failures.append("Redis")
            except Exception as e:
                logger.error(f"Failed to establish Redis connection: {e}")
                connection_failures.append("Redis")

        # Database connection
        if self._requirements["db"]:
            try:
                logger.info(f"{self.__class__.__name__} initializing Database connection")
                self.db = AsyncDatabaseManager()
                await self.db.initialize()
                checkers.append(MariaDBHealthChecker(self.db))
                logger.debug("Database connection established")
            except Exception as e:
                logger.error(f"Failed to establish Database connection: {e}")
                connection_failures.append("Database")

        # RabbitMQ connection
        if self._requirements["rabbit"]:
            try:
                logger.info(f"{self.__class__.__name__} initializing RabbitMQ connection")
                self.rabbit = AsyncRabbitMQClient()
                await self.rabbit.connect()
                checkers.append(RabbitMQHealthChecker(self.rabbit))
                logger.debug("RabbitMQ connection established")
            except Exception as e:
                logger.error(f"Failed to establish RabbitMQ connection: {e}")
                connection_failures.append("RabbitMQ")

        # Establish health check registry
        self._registry = HealthCheckRegistry(*checkers)

        # Report connection status with understated elegance
        if connection_failures:
            logger.warning(f"One or more connections failed: {', '.join(connection_failures)}")
        else:
            logger.info(f"All required connections established successfully")

        # Allow subclasses to perform additional setup
        await self.service_setup()

    @abstractmethod
    async def service_setup(self):
        """Subclass-specific setup logic."""
        pass

    async def initialise(self):
        """Validates all services are healthy before operation commences."""
        logger.info(f"Initializing {self.__class__.__name__}")
        await self._registry.assert_healthy_or_exit()
        logger.info(f"{self.__class__.__name__} initialized successfully")

    @abstractmethod
    async def process_batch(self, batch_size: int = 100, timeout: float = 30.0) -> int:
        """
        Process a batch of items with sophisticated error handling.

        Args:
            batch_size: Maximum number of items to process
            timeout: Maximum processing time in seconds

        Returns:
            int: Number of items successfully processed
        """
        pass

    async def start(self, interval_seconds: int = 60):
        """Begins the continuous processing loop with appropriate task management."""
        logger.info(f"Starting {self.__class__.__name__}")
        self._running = True

        # Use a task to allow cancellation during shutdown
        self._task = asyncio.create_task(self._processing_loop(interval_seconds))
        return self._task

    async def _processing_loop(self, interval_seconds):
        """Internal long-running loop that processes batches on schedule."""
        while self._running:
            try:
                items_processed = await self.process_batch()
                log_level = logging.INFO if items_processed > 0 else logging.DEBUG
                logger.log(log_level, f"Processed {items_processed} items")
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                logger.info(f"{self.__class__.__name__} processing loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                # Reduced interval for error recovery
                await asyncio.sleep(interval_seconds / 2)

    async def stop(self):
        """Gracefully stops the service and releases connections with aristocratic poise."""
        logger.info(f"Stopping {self.__class__.__name__}")
        self._running = False

        if self._task:
            try:
                self._task.cancel()
                await asyncio.wait_for(asyncio.shield(self._task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.debug("Task cancellation confirmed")
            except Exception as e:
                logger.warning(f"Exception during task cancellation: {e}")

        # Close connections in reverse order of importance
        if self.rabbit:
            try:
                await self.rabbit.close()
                logger.debug("RabbitMQ connection closed")
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ connection: {e}")

        if self.db:
            try:
                await self.db.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing Database connection: {e}")

        if self.redis:
            try:
                await self.redis.close()
                logger.debug("Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")

        logger.info(f"{self.__class__.__name__} stopped successfully")
