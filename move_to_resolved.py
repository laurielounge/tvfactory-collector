# migrate_queues.py (high-speed version)

import asyncio

from aio_pika import connect_robust, Message

from config.settings import settings
from core.logger import logger


async def migrate_queue_fast(source: str, target: str):
    logger.info(f"Starting fast migration: {source} → {target}")
    count = 0

    connection = await connect_robust(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        login=settings.RABBITMQ_USER,
        password=settings.RABBITMQ_PASSWORD,
        virtualhost=settings.RABBITMQ_VHOST,
    )
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=500)

    queue = await channel.declare_queue(source, durable=True)
    exchange = channel.default_exchange

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                try:
                    await exchange.publish(
                        Message(body=message.body),
                        routing_key=target
                    )
                    count += 1
                    if count % 1000 == 0:
                        logger.info(f"Migrated {count} from {source}")
                except Exception as e:
                    logger.error(f"Error on message: {e}", exc_info=True)

    logger.info(f"✅ Finished fast migration: {count} messages from {source} to {target}")
    await connection.close()


async def main():
    # await migrate_queue_fast("impressions_queue", "resolved_impressions_queue")
    await migrate_queue_fast("resolved_impressions_queue", "raw_impressions_queue")


if __name__ == "__main__":
    asyncio.run(main())
