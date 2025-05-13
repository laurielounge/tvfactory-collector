import asyncio
from infrastructure.rabbitmq_client import AsyncRabbitMQClient

async def declare_empty_queues():
    rabbit = AsyncRabbitMQClient()
    await rabbit.connect()
    await rabbit.channel.declare_queue("resolved_impressions_queue", durable=True)
    await rabbit.channel.declare_queue("resolved_webhits_queue", durable=True)
    await rabbit.channel.declare_queue("raw_impressions_queue", durable=True)
    await rabbit.channel.declare_queue("raw_webhits_queue", durable=True)
    await rabbit.close()

asyncio.run(declare_empty_queues())
