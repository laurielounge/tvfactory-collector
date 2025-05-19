# /opt/collector_service/service.py
import argparse
import asyncio
import signal
import sys
import time

from dotenv import load_dotenv

from config.settings import settings
from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from services.impression_consumer import ImpressionConsumerService
from services.loghit_worker import LoghitWorkerService
from services.parallel_loghit_worker import ParallelLoghitWorkerService
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


async def run_parallel_loghit(num_workers=4, batch_size=5000, interval_seconds=1):
    """Run multiple loghit workers in parallel."""
    logger.info(f"Starting parallel LoghitWorker with {num_workers} workers")
    service = ParallelLoghitWorkerService(num_workers=num_workers)
    await service.initialise()
    active_services.append(service)
    await service.start(batch_size=batch_size, interval_seconds=interval_seconds)
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
            # Phase 1: Process loghit_queue
            logger.info("[SEQUENCE] Processing loghit_queue (max 30 seconds)")
            count = 0
            max_items = 20000
            start_time = time.time()
            max_duration = 30

            while (time.time() - start_time < max_duration) and (count < max_items):
                batch_count = await loghit_worker.process_batch(batch_size=500)
                count += batch_count

                if batch_count < settings.LOW_VOLUME_THRESHOLD:
                    logger.info(f"[SEQUENCE] Loghit processed low-volume batch ({batch_count}). Pausing.")
                    await asyncio.sleep(settings.LOW_VOLUME_PAUSE_SECONDS)
                    break

            duration = time.time() - start_time
            logger.info(f"[SEQUENCE] Processed {count} log entries in {duration:.1f}s ({count / duration:.1f}/s)")

            # Phase 2: Process impressions
            logger.info("[SEQUENCE] Processing raw_impressions_queue")
            imp_count = 0
            imp_start = time.time()
            max_imp_time = 30

            try:
                queue_length = await loghit_worker.rabbit.get_queue_length("raw_impressions_queue")
                logger.info(f"[SEQUENCE] Found {queue_length} impressions to process")
            except Exception as e:
                logger.error(f"Error getting impression queue length: {e}")
                queue_length = 0

            while (time.time() - imp_start < max_imp_time) and (imp_count < max_items):
                batch_count = await impression_service.process_batch(batch_size=500)
                imp_count += batch_count

                if batch_count < settings.LOW_VOLUME_THRESHOLD:
                    logger.info(f"[SEQUENCE] Low-volume impression batch ({batch_count}). Pausing.")
                    await asyncio.sleep(settings.LOW_VOLUME_PAUSE_SECONDS)
                    break

            imp_duration = time.time() - imp_start
            if imp_count > 0:
                logger.info(
                    f"[SEQUENCE] Processed {imp_count} impressions in {imp_duration:.1f}s ({imp_count / imp_duration:.1f}/s)")
            else:
                logger.info("[SEQUENCE] No impressions processed")

            # Phase 3: Process webhits
            logger.info("[SEQUENCE] Processing raw_webhits_queue")
            hit_count = 0
            hit_start = time.time()
            max_hit_time = 30

            try:
                queue_length = await loghit_worker.rabbit.get_queue_length("raw_webhits_queue")
                logger.info(f"[SEQUENCE] Found {queue_length} webhits to process")
            except Exception as e:
                logger.error(f"Error getting webhits queue length: {e}")
                queue_length = 0

            while (time.time() - hit_start < max_hit_time) and (hit_count < max_items):
                batch_count = await webhit_service.process_batch(batch_size=500)
                hit_count += batch_count

                if batch_count < settings.LOW_VOLUME_THRESHOLD:
                    logger.info(f"[SEQUENCE] Low-volume webhits batch ({batch_count}). Pausing.")
                    await asyncio.sleep(settings.LOW_VOLUME_PAUSE_SECONDS)
                    break

            hit_duration = time.time() - hit_start
            if hit_count > 0:
                logger.info(
                    f"[SEQUENCE] Processed {hit_count} webhits in {hit_duration:.1f}s ({hit_count / hit_duration:.1f}/s)")
            else:
                logger.info("[SEQUENCE] No webhits processed")

            # Exit if --run-once and something was processed
            if run_once and (count > 0 or imp_count > 0 or hit_count > 0):
                break

            logger.info("[SEQUENCE] Sleeping before next cycle")
            await asyncio.sleep(5)

    try:
        await sequence_loop()
    finally:
        # Ensure proper cleanup
        await graceful_shutdown()


async def run_pipeline(num_workers=3):
    """Run all services in parallel as a continuous processing pipeline."""
    logger.info(f"Starting continuous processing pipeline with {num_workers} loghit workers")

    # Start loghit workers
    loghit_service = ParallelLoghitWorkerService(num_workers=num_workers)
    await loghit_service.initialise()

    # Start impression consumer
    impression_service = await ImpressionConsumerService.create()
    await impression_service.initialise()

    # Start webhit consumer
    webhit_service = await WebhitConsumerService.create()
    await webhit_service.initialise()

    # Add all to active services
    active_services.extend([loghit_service, impression_service, webhit_service])

    # Start all services in parallel
    await asyncio.gather(
        loghit_service.start(batch_size=5000, interval_seconds=1),
        impression_service.start(batch_size=5000, interval_seconds=5),
        webhit_service.start(batch_size=5000, interval_seconds=5)
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, help="loghit | impression | webhit | sequence | pipeline")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for processing")
    parser.add_argument("--interval", type=int, help="Interval between processing cycles")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing (only for loghit)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (with --parallel)")
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
        if args.role == "loghit":
            if args.parallel:
                logger.info(f"Starting parallel loghit processing with {args.workers} workers")
                await run_parallel_loghit(
                    num_workers=args.workers,
                    batch_size=args.batch_size,
                    interval_seconds=args.interval
                )
            else:
                await run_service(LoghitWorkerService,
                                  run_once=args.once,
                                  batch_size=args.batch_size,
                                  # interval_seconds=args.interval
                                  )

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

        elif args.role == "pipeline":
            await run_pipeline(num_workers=args.workers)
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
