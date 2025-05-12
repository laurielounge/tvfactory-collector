# /opt/collector_service/service.py
import argparse
import asyncio
import sys

from dotenv import load_dotenv

from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from services.impression_consumer import ImpressionConsumerService
from services.log_collector_service import LogCollectorService
from services.loghit_worker import LoghitWorkerService
from services.webhit_consumer import WebhitConsumerService

load_dotenv()
worker = None


def graceful_shutdown():
    logger.info("Shutdown signal received. Arranging a graceful exit...")
    if worker and hasattr(worker, "stop"):
        asyncio.create_task(worker.stop())


async def main():
    global worker
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, help="collector | loghit | impression | webhit | sequence")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    if args.once:
        import logging
        logger.setLevel(logging.DEBUG)
        logger.debug("Run-once mode detected — log level set to DEBUG.")

    db_manager = AsyncDatabaseManager()
    rabbitmq_client = AsyncRabbitMQClient()
    await rabbitmq_client.connect()
    await db_manager.initialize()

    logger.info(f"Host name to check we read .env: {sys.platform}")

    if args.role == "collector":
        logger.info("Running log collector worker")
        worker = await LogCollectorService.create()
        await worker.start(interval_seconds=300, run_once=args.once)

    elif args.role == "loghit":
        logger.info("Running loghit worker")
        worker = await LoghitWorkerService.create()
        await worker.start(batch_size=5000, interval_seconds=1, run_once=args.once)

    elif args.role == "impression":
        logger.info("Running impression consumer")
        try:
            service = await ImpressionConsumerService.create()
            await service.start(run_once=args.once)
        except Exception as e:
            import traceback
            logger.error(f"Unhandled exception: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    elif args.role == "webhit":
        logger.info("Running webhit worker")
        worker = await WebhitConsumerService.create()
        await worker.start(run_once=args.once)

    elif args.role == "sequence":
        logger.info("Running coordinated sequence: loghit → impression → webhit")
        loghit_worker = await LoghitWorkerService.create()
        imp_service = await ImpressionConsumerService.create()
        webhit_service = await WebhitConsumerService.create()
        rabbit = rabbitmq_client  # already connected earlier in main()

        async def sequence_loop():
            while True:
                # Phase 1: Drain loghit_queue
                logger.info("[SEQUENCE] Draining loghit_queue (Redis)")
                count = 0
                while True:
                    batch = await loghit_worker._process_batch(1000)
                    if batch == 0:
                        break
                    count += batch
                logger.info(f"[SEQUENCE] loghit_queue drained — {count} lines processed")

                # Phase 2: Drain impressions_queue
                logger.info("[SEQUENCE] Draining impressions_queue")
                while await rabbit.get_queue_length("impressions_queue") > 0:
                    await imp_service.run_once(max_messages=1000)
                logger.info("[SEQUENCE] impressions_queue drained")

                # Phase 3: Drain webhits_queue
                logger.info("[SEQUENCE] Draining webhits_queue")
                while await rabbit.get_queue_length("webhits_queue") > 0:
                    await webhit_service.run_once(max_messages=1000)
                logger.info("[SEQUENCE] webhits_queue drained")
                if args.once:
                    break
                logger.info("[SEQUENCE] Sleeping before next cycle")
                await asyncio.sleep(15)

        await sequence_loop()

    else:
        logger.error(f"Unknown role: {args.role}")
        sys.exit(1)

    await db_manager.close()
    await rabbitmq_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted by user.")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)
    finally:
        logger.info("Service shut down")
