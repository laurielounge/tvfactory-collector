# service.py
import asyncio
import signal
import sys
from functools import partial

from dotenv import load_dotenv
from services.collector_service import AsyncCollectionService

from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient

# Load environment variables with understated efficiency
load_dotenv()

# Global reference to our service for shutdown handling
collector = None


def graceful_shutdown(signum, frame, loop):
    """Handle shutdown with appropriate poise"""
    logger.info("Shutdown signal received. Arranging a graceful exit...")

    if collector:
        # Schedule the shutdown in the event loop
        asyncio.create_task(shutdown())
    else:
        # No collector to shut down, exit directly
        sys.exit(0)


async def shutdown():
    """Orchestrate an elegant shutdown sequence"""
    try:
        if collector:
            await collector.stop()
            logger.info("Collection service stopped gracefully")

        # Allow time for final operations to complete
        await asyncio.sleep(2)
    finally:
        # Stop the event loop
        loop = asyncio.get_event_loop()
        loop.stop()


async def main():
    """Main entry point with asynchronous sophistication"""
    global collector

    try:
        # Initialize our essential components
        db_manager = AsyncDatabaseManager()
        rabbitmq_client = AsyncRabbitMQClient()

        # Establish connections before service initialization
        await rabbitmq_client.connect()
        await db_manager.initialize()

        # Create and initialize the collection service
        collector = AsyncCollectionService(db_manager, rabbitmq_client)
        await collector.initialize()

        logger.info("Collection service initialized. Beginning collection cycle.")

        # Start the collection process with a 30-second interval
        await collector.start(interval_seconds=30)
    except Exception as e:
        logger.error(f"Fatal error in collection service: {e}")
        sys.exit(1)
    finally:
        # Ensure proper cleanup
        if hasattr(db_manager, 'close'):
            await db_manager.close()
        if hasattr(rabbitmq_client, 'close'):
            await rabbitmq_client.close()


if __name__ == "__main__":
    # Get the event loop
    loop = asyncio.get_event_loop()

    # Register signal handlers with elegant efficiency
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            partial(graceful_shutdown, sig, None, loop)
        )

    try:
        # Run the main async function
        loop.run_until_complete(main())
        loop.run_forever()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
    finally:
        # Final cleanup
        loop.close()
        logger.info("Collection service shut down")
