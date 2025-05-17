# infrastructure/rabbitmq_client.py
import asyncio
import json
from datetime import datetime
from json import JSONEncoder
from typing import Callable, Any, Optional

import aio_pika

from config.config import settings
from core.logger import logger


class AsyncRabbitMQClient:
    """Sophisticated asynchronous RabbitMQ interface."""

    def __init__(self):
        self.rabbitmq_host = settings.RABBITMQ_HOST
        self.rabbitmq_vhost = settings.RABBITMQ_VHOST
        self.rabbitmq_user = settings.RABBITMQ_USER
        self.rabbitmq_pass = settings.RABBITMQ_PASSWORD
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self._connection_lock = asyncio.Lock()
        self._consumers = {}
        self._closed = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self) -> None:
        """Establish connection to RabbitMQ with elegant error handling."""
        async with self._connection_lock:
            if self.connection and not self.connection.is_closed:
                return  # Connection already established
            try:
                # Create robust connection that handles reconnections gracefully
                self.connection = await aio_pika.connect_robust(
                    host=self.rabbitmq_host,
                    virtualhost=self.rabbitmq_vhost,
                    login=self.rabbitmq_user,
                    password=self.rabbitmq_pass,
                    heartbeat=600
                )

                # Create channel with prefetch for balanced consumption
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=100)

                # Declare standard queues
                await self.declare_standard_queues()

                logger.info(f"Connected to RabbitMQ at {self.rabbitmq_host}")
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                raise

    async def declare_standard_queues(self) -> None:
        """Declare our standard set of queues with appropriate durability."""
        if not self.channel:
            await self.connect()

        # Declare input queues
        await self.channel.declare_queue('impressions_queue', durable=True)
        await self.channel.declare_queue('webhits_queue', durable=True)
        # Declare result queues
        await self.channel.declare_queue('resolved_impressions_queue', durable=True)
        await self.channel.declare_queue('resolved_webhits_queue', durable=True)

    async def declare_queue(self, queue_name, durable=True) -> None:
        """Declare our standard set of queues with appropriate durability."""
        if not self.channel:
            await self.connect()

        # Declare input queues
        await self.channel.declare_queue(queue_name, durable=durable)

    async def consume(self, queue_name: str, callback: Callable) -> None:
        """Begin consumption from specified queue with callback processing."""
        if not self.channel:
            await self.connect()
        logger.debug(f"[CONSUME] Connected: {not self.channel.is_closed} | Queue: {queue_name}")
        queue = await self.channel.declare_queue(queue_name, durable=True)
        consumer_tag = await queue.consume(callback)
        self._consumers[queue_name] = consumer_tag

        logger.debug(f"Started consuming from {queue_name}")

    async def get_batch(self, queue_name: str, batch_size: int = 500) -> list:
        """
        Retrieves a batch of messages with sophisticated error handling.

        Args:
            queue_name: The queue to retrieve messages from
            batch_size: Maximum number of messages to retrieve

        Returns:
            List of messages, empty if none available
        """
        if not self.channel:
            await self.connect()

        messages = []

        try:
            # Declare the queue passively - confirm existence without creation
            queue = await self.channel.declare_queue(
                queue_name,
                durable=True
            )

            # Set prefetch for optimal throughput
            await self.channel.set_qos(prefetch_count=batch_size)

            # Efficient batch retrieval with timeout
            try:
                async with queue.iterator(timeout=1.0) as queue_iter:
                    for _ in range(batch_size):
                        try:
                            # Use a small timeout to avoid blocking too long
                            message = await asyncio.wait_for(queue_iter.__anext__(), timeout=0.05)
                            messages.append(message)

                            # Log batch progress with aristocratic restraint
                            if len(messages) % 100 == 0:
                                logger.debug(f"Retrieved {len(messages)} messages from {queue_name}")

                        except (asyncio.TimeoutError, StopAsyncIteration):
                            # No more messages available - conclude with composure
                            break
                        except Exception as e:
                            logger.error(f"Error retrieving message from {queue_name}: {e}")
                            break
            except Exception as e:
                logger.error(f"Error iterating queue {queue_name}: {e}")

            # Log batch retrieval results
            if messages:
                logger.info(f"Retrieved batch of {len(messages)} messages from {queue_name}")
            elif batch_size > 0:
                logger.debug(f"No messages available from {queue_name}")

            return messages

        except Exception as e:
            logger.error(f"Failed to get batch from {queue_name}: {e}")
            return []

    async def publish(self, exchange: str, routing_key: str, message: Any) -> None:
        """Publish message with sophisticated error handling."""

        class EnhancedJSONEncoder(JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)

        if not self.channel:
            await self.connect()

        try:
            # Convert dict to JSON string if necessary
            if isinstance(message, dict):
                message = json.dumps(message, cls=EnhancedJSONEncoder)

            # Ensure message is bytes
            if isinstance(message, str):
                message = message.encode()

            # Create message with persistence
            message_obj = aio_pika.Message(
                body=message,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            # Get the exchange (empty string for default)
            target_exchange = self.channel.default_exchange if not exchange else \
                await self.channel.declare_exchange(exchange, aio_pika.ExchangeType.DIRECT)

            # Publish with confirmation
            await target_exchange.publish(
                message_obj,
                routing_key=routing_key
            )
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise

    async def get_message(self, queue_name: str):
        """Retrieves a single message with understated efficiency."""
        if not self.channel:
            await self.connect()

        # Declare the queue passively - confirm existence without creation
        queue = await self.channel.declare_queue(
            queue_name,
            passive=True  # Don't create, merely verify
        )

        # Get a single message, if available
        async with queue.iterator(timeout=0.1) as queue_iter:
            async for message in queue_iter:
                return message  # Return just the first message

        # Queue was empty - return with dignified silence
        return None

    async def get_queue_length(self, queue_name: str) -> int:
        """Retrieve message count from a queue."""
        if not self.channel:
            await self.connect()
        try:
            queue = await self.channel.declare_queue(queue_name, passive=True)
            return queue.declaration_result.message_count
        except Exception as e:
            logger.warning(f"Could not get length for {queue_name}: {e}")
            return 0

    async def close(self) -> None:
        """Close connection with characteristic grace."""
        if self._closed:
            return
        self._closed = True

        if self.connection and not self.connection.is_closed:
            for queue_name, consumer_tag in self._consumers.items():
                try:
                    await self.channel.cancel(consumer_tag)
                    logger.debug(f"Cancelled consumer for {queue_name}")
                except Exception as e:
                    logger.warning(f"Error cancelling consumer for {queue_name}: {e}")

            await self.connection.close()
            await asyncio.sleep(0.1)  # Let aio-pika clean up properly
            logger.info("RabbitMQ connection closed")
