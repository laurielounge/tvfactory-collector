# collector.py
import signal
import sys
import time

from dotenv import load_dotenv

from core.logger import logger
from infrastructure.database import get_db_manager
from services.collector_service import CollectionService

# Load environment variables from existing .env
load_dotenv()


def graceful_shutdown(signum, frame):
    """Handle shutdown with appropriate poise"""
    logger.info("Shutdown signal received. Exiting gracefully...")
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers with quiet efficiency
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # Initialize the collection service using your existing DB manager
    db_manager = get_db_manager()
    collector = CollectionService(db_manager)

    logger.info("Collection service initialized. Beginning collection cycle.")

    # Run the collection loop with aristocratic composure
    try:
        while True:
            collector.collect_and_queue()
            time.sleep(30)  # Adjusted based on throughput requirements
    except Exception as e:
        logger.error(f"Fatal error in collection service: {e}")
        sys.exit(1)
