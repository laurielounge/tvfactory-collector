# services/parallel_loghit_worker.py

import asyncio

from core.logger import logger
from services.loghit_worker import LoghitWorkerService


class ParallelLoghitWorkerService:
    """Manages multiple LoghitWorker instances for parallel processing"""

    def __init__(self, num_workers=4):
        self.num_workers = num_workers
        self.workers = []
        self.tasks = []

    async def initialise(self):
        """Create and initialize all worker instances"""
        logger.info(f"Initializing {self.num_workers} parallel LoghitWorker instances")

        self.workers = []
        for i in range(self.num_workers):
            worker = await LoghitWorkerService.create()
            await worker.initialise()
            self.workers.append(worker)

        logger.info(f"All {self.num_workers} workers initialized successfully")
        return True

    async def start(self, batch_size=1000, interval_seconds=1):
        """Start all workers with staggered intervals to prevent thundering herd"""
        logger.info(f"Starting {self.num_workers} parallel workers")

        self.tasks = []
        for i, worker in enumerate(self.workers):
            # Stagger starts by 100ms to prevent all workers hitting Redis simultaneously
            await asyncio.sleep(0.1 * i)

            # Create task for each worker
            task = asyncio.create_task(
                worker.start(batch_size=batch_size, interval_seconds=interval_seconds),
                name=f"loghit_worker_{i}"
            )
            self.tasks.append(task)

        logger.info(f"All {self.num_workers} workers started")

        # Wait for all tasks to complete (they normally won't unless stopped)
        await asyncio.gather(*self.tasks)

    async def stop(self):
        """Stop all worker instances gracefully"""
        logger.info(f"Stopping {self.num_workers} parallel workers")

        stop_tasks = []
        for worker in self.workers:
            stop_tasks.append(worker.stop())

        await asyncio.gather(*stop_tasks)

        # Cancel any remaining tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        logger.info("All parallel workers stopped")
