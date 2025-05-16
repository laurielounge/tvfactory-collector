# /opt/collector_service/service.py
import argparse
import asyncio
import signal
import sys

from dotenv import load_dotenv

from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from services.impression_consumer import ImpressionConsumerService
from services.log_collector_service import LogCollectorService
from services.loghit_worker import LoghitWorkerService
from services.webhit_consumer import WebhitConsumerService

load_dotenv()
active_services = []  # Track all active services for graceful shutdown


async def graceful_shutdown(signum=None, frame=None):
    """Coordinate graceful shutdown of all running services."""
    logger.info(f"Shutdown signal received ({signum if signum else 'manual'}). Arranging a graceful exit...")

    shutdown_tasks = []
    for service in active_services:
        if hasattr(service, "stop"):
            shutdown_tasks.append(service.stop())

    if shutdown_tasks:
        logger.info(f"Stopping {len(shutdown_tasks)} active services...")
        await asyncio.gather(*shutdown_tasks)
        logger.info("All services stopped gracefully")


async def run_service(service_class, run_once=False, **kwargs) -> BaseAsyncFactory:
    """Standard pattern for creating, initializing and running a service."""
    service = await service_class.create(run_once=run_once)
    await service.initialise()  # Explicitly call initialize to validate health

    active_services.append(service)

    if run_once:
        # For run_once, we use process_batch directly
        logger.info(f"Running {service_class.__name__} once")
        count = await service.process_batch(**kwargs)
        logger.info(f"Processed {count} items")
        # No need to stop as we're not starting a continuous task
    else:
        # For continuous operation, use start
        logger.info(f"Starting {service_class.__name__} in continuous mode")
        await service.start(**kwargs)

    return service


async def run_sequence(run_once=False):
    """Run all services in coordinated sequence with proper cleanup."""
    logger.info("Running coordinated sequence: loghit → impression → webhit")

    # Create all services
    loghit_worker = await LoghitWorkerService.create(run_once=True)  # Always run_once=True for sequence mode
    impression_service = await ImpressionConsumerService.create(run_once=True)
    webhit_service = await WebhitConsumerService.create(run_once=True)

    # Initialize all services
    await loghit_worker.initialise()
    await impression_service.initialise()
    await webhit_service.initialise()

    # Add to active services for graceful shutdown
    active_services.extend([loghit_worker, impression_service, webhit_service])

    async def sequence_loop():
        while True:
            # Phase 1: Drain loghit_queue
            logger.info("[SEQUENCE] Draining loghit_queue (Redis)")
            count = 0
            while True:
                batch_count = await loghit_worker.process_batch(batch_size=1000)
                if batch_count == 0:
                    break
                count += batch_count
            logger.info(f"[SEQUENCE] loghit_queue drained — {count} lines processed")

            # Phase 2: Drain impressions_queue
            logger.info("[SEQUENCE] Draining raw_impressions_queue")
            queue_length = await loghit_worker.rabbit.get_queue_length("raw_impressions_queue")
            while queue_length > 0:
                await impression_service.process_batch(batch_size=1000)
                queue_length = await loghit_worker.rabbit.get_queue_length("raw_impressions_queue")
            logger.info("[SEQUENCE] raw_impressions_queue drained")

            # Phase 3: Drain webhits_queue
            logger.info("[SEQUENCE] Draining raw_webhits_queue")
            queue_length = await loghit_worker.rabbit.get_queue_length("raw_webhits_queue")
            while queue_length > 0:
                await webhit_service.process_batch(batch_size=1000)
                queue_length = await loghit_worker.rabbit.get_queue_length("raw_webhits_queue")
            logger.info("[SEQUENCE] raw_webhits_queue drained")

            if run_once:
                break

            logger.info("[SEQUENCE] Sleeping before next cycle")
            await asyncio.sleep(30)

    try:
        await sequence_loop()
    finally:
        # Ensure proper cleanup
        await graceful_shutdown()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, help="collector | loghit | impression | webhit | sequence")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for processing")
    parser.add_argument("--interval", type=int, help="Interval between processing cycles")
    args = parser.parse_args()

    if args.once:
        import logging
        logger.setLevel(logging.DEBUG)
        logger.debug("Run-once mode detected — log level set to DEBUG.")

    # Set default interval based on role if not specified
    if not args.interval:
        args.interval = {
            "collector": 300,  # 5 minutes
            "loghit": 1,  # 1 second
            "impression": 5,  # 5 seconds
            "webhit": 5,  # 5 seconds
        }.get(args.role, 30)  # 30 seconds default

    try:
        if args.role == "collector":
            await run_service(LogCollectorService,
                              run_once=args.once,
                              interval_seconds=args.interval)

        elif args.role == "loghit":
            await run_service(LoghitWorkerService,
                              run_once=args.once,
                              batch_size=args.batch_size,
                              interval_seconds=args.interval)

        elif args.role == "impression":
            await run_service(ImpressionConsumerService,
                              run_once=args.once,
                              interval_seconds=args.interval)

        elif args.role == "webhit":
            await run_service(WebhitConsumerService,
                              run_once=args.once,
                              batch_size=args.batch_size)

        elif args.role == "sequence":
            await run_sequence(run_once=args.once)

        else:
            logger.error(f"Unknown role: {args.role}")
            sys.exit(1)

    except Exception as e:
        import traceback
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda s, f: asyncio.create_task(graceful_shutdown(s, f)))

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted by user.")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
    finally:
        # Ensure any remaining cleanup happens
        if active_services:
            asyncio.run(graceful_shutdown())
        logger.info("Service shut down")
