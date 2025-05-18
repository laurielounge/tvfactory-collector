import asyncio

from core.logger import logger
from infrastructure.redis_client import get_redis_client


async def initialize_counters():
    """Initialize Redis counters for ID generation if not already set."""
    redis_client = await get_redis_client()

    # Check if counters exist
    imp_id = await redis_client.get("global:next_impression_id")
    webhit_id = await redis_client.get("global:next_webhit_id")

    # Set with defaults if not set
    if not imp_id:
        await redis_client.set("global:next_impression_id", 41588276)
        logger.info("Initialized impression ID counter to 41588276")

    if not webhit_id:
        await redis_client.set("global:next_webhit_id", 1122462)
        logger.info("Initialized webhit ID counter to 1122462")



if __name__ == "__main__":
    asyncio.run(initialize_counters())

