#!/usr/bin/env python
# test_impression_consumer.py

import asyncio
import logging
import sys

# Configure precise logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    stream=sys.stdout
)


# The elegance of direct execution
async def main():
    """Execute the impression consumer in isolation."""
    from infrastructure.redis_client import get_redis_client
    from infrastructure.rabbitmq_client import AsyncRabbitMQClient
    from services.impression_consumer import ImpressionConsumerService

    print("Initializing components...")
    redis_client = await get_redis_client()
    rabbitmq = AsyncRabbitMQClient()
    await rabbitmq.connect()

    print("Creating service instance...")
    service = ImpressionConsumerService(redis_client, rabbitmq)

    print("Executing run_once with limited batch size...")
    try:
        # The threshold of truth
        processed = await service.run_once(max_messages=10)
        print(f"Successfully processed {processed} messages")
    except Exception as e:
        print(f"Exception encountered: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # One must always clean up properly
        print("Closing connections...")
        await rabbitmq.close()
        # Close any other resources as needed


# The simplicity of direct execution
if __name__ == "__main__":
    print("Commencing test execution...")
    asyncio.run(main())
    print("Test execution complete.")