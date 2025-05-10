# utils/redis_client.py

import redis.asyncio as redis

from config.config import settings
from core.logger import logger

_client = None


async def get_redis_client():
    """
    Creates and returns a singleton async Redis client connection.
    Returns:
        redis.Redis: Connected async Redis client instance
    """
    global _client
    if _client is None:
        try:
            _client = redis.Redis(
                host=settings.REDIS_HOST,
                port=int(settings.REDIS_PORT),
                password=settings.REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=float(settings.REDIS_TIMEOUT),
            )
            logger.debug("Async Redis client initialized")
        except Exception as e:
            logger.error(f"Error creating async Redis connection: {e}")
            return None
    return _client


def redis_safe(func):
    """
    Decorator for Redis operations that handles exceptions gracefully.
    Wraps Redis functions to catch and log exceptions, returning None
    instead of letting exceptions propagate.
    Args:
        func: The function to decorate
    Returns:
        wrapper: The decorated function that handles Redis exceptions
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{func.__name__} failed: {e}")
            return None

    return wrapper
