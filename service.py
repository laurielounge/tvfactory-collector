# serviuce.py
import argparse
import asyncio
import signal
import sys
from functools import partial

from dotenv import load_dotenv

from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from services.log_collector_service import LogCollectorService
from services.loghit_worker import LoghitWorkerService

worker = None
loop = asyncio.get_event_loop()
load_dotenv()


def graceful_shutdown(signum, frame):
    logger.info("Shutdown signal received. Arranging a graceful exit...")
    if worker and hasattr(worker, "stop"):
        loop.create_task(worker.stop())

    for task in asyncio.all_tasks(loop):
        task.cancel()
    loop.stop()


async def main():
    global worker

    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, help="collector | loghit")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    db_manager = AsyncDatabaseManager()
    rabbitmq_client = AsyncRabbitMQClient()
    await rabbitmq_client.connect()
    await db_manager.initialize()

    logger.info(f"Host name to check we read .env: {sys.platform}")

    if args.role == "collector":
        logger.info("Running log collector worker")
        worker = LogCollectorService()
        await worker.start(interval_seconds=30, run_once=args.once)

    elif args.role == "loghit":
        logger.info("Running loghit worker")
        worker = LoghitWorkerService()
        await worker.start(batch_size=100, interval_seconds=1, run_once=args.once)

    else:
        logger.error(f"Unknown role: {args.role}")
        sys.exit(1)

    await db_manager.close()
    await rabbitmq_client.close()


if __name__ == "__main__":
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, partial(graceful_shutdown, sig, None))

    try:
        loop.run_until_complete(main())
    except asyncio.CancelledError:
        logger.info("Cancelled by signal.")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
    finally:
        loop.close()
        logger.info("Service shut down")
