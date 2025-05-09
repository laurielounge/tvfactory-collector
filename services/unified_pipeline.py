# services/unified_pipeline.py
import asyncio

from core.logger import logger
from services.log_collector_service import LogCollectorService
from services.loghit_worker import LoghitWorkerService


class UnifiedPipelineService:
    def __init__(self):
        self.collector = LogCollectorService()
        self.loghit = LoghitWorkerService()

    async def run_once(self):
        logger.info("UnifiedPipelineService: Starting one complete run.")

        await self.collector.start(run_once=True)
        await self.loghit.start(batch_size=1000, interval_seconds=0.1)

        logger.info("UnifiedPipelineService: Finished one complete run.")

    async def run_forever(self, interval_seconds=60):
        while True:
            await self.run_once()
            await asyncio.sleep(interval_seconds)
