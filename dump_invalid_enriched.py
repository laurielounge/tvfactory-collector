import asyncio
import json

from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from core.logger import logger

async def purge_invalid_messages():
    """Purge messages with missing region_id from the queue."""
    client = AsyncRabbitMQClient()
    await client.connect()

    batch_size = 1000
    invalid_count = 0
    processed = 0

    while True:
        messages = await client.get_batch("enriched_webhits_queue", batch_size)
        if not messages:
            break

        for message in messages:
            processed += 1
            try:
                data = json.loads(message.body)
                if data.get('ip_resolution', {}).get('region_id') is None:
                    await message.nack(requeue=False)  # Discard invalid message
                    invalid_count += 1
                else:
                    await message.nack(requeue=True)  # Put it back in the queue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await message.nack(requeue=False)

        logger.info(f"Processed {processed} messages, found {invalid_count} invalid")

    await client.close()

async def main():
    # await migrate_queue_fast("impressions_queue", "resolved_impressions_queue")
    await purge_invalid_messages()


if __name__ == "__main__":
    asyncio.run(main())