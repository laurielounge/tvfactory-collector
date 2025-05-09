# services/collection_service.py
import asyncio
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from infrastructure.rabbitmq_client import AsyncRabbitMQClient
from models.impression import SourceImpression
from models.ping import SourceLastSiteResponse, TargetLastSiteResponse
from models.processing_state import ProcessingState
from models.webhit import SourceWebHit

BATCH_SIZE = 500
from utils.timer import StepTimer

timer = StepTimer()


class AsyncCollectionService:
    """Orchestrates data collection with asynchronous efficiency"""

    def __init__(self, db_manager: AsyncDatabaseManager, rabbitmq_client: AsyncRabbitMQClient):
        self.db_manager = db_manager
        self.rabbitmq_client = rabbitmq_client
        self.last_run = datetime.now() - timedelta(days=10)
        # Track last processed IDs
        self.last_impression_id = 0
        self.last_webhit_id = 0
        self.running = False
        self._initialized = False

    async def initialize(self):
        """Prepare the service for operation with understated grace"""
        if self._initialized:
            return

        # Ensure RabbitMQ connection is established
        await self.rabbitmq_client.connect()

        # Retrieve last processed IDs
        await self._initialize_last_ids()

        self._initialized = True
        logger.info("Collection service initialized successfully")

    async def _initialize_last_ids(self):
        """Retrieve the last processed IDs with elegant persistence"""
        try:
            # First attempt to get IDs from our processing state table
            async with self.db_manager.async_session_scope('TVFACTORY') as session:
                result = await session.execute(
                    select(ProcessingState).where(
                        ProcessingState.entity_type.in_(['impression', 'webhit'])
                    )
                )
                states = {state.entity_type: state.last_id for state in result.scalars().all()}

                if states:
                    self.last_impression_id = states.get('impression', 0)
                    self.last_webhit_id = states.get('webhit', 0)
                    logger.info(
                        f"Retrieved processing state: impression ID {self.last_impression_id}, webhit ID {self.last_webhit_id}")
                    return

            # If no states found, initialize from source database as fallback
            source_db = await self.db_manager.try_async_source_connections(['TVBVODDB1', 'TVBVODDB2'])
            if source_db:
                async with self.db_manager.async_session_scope(source_db) as session:
                    # Get the last impression ID with reasonable safety margin
                    result = await session.execute(select(func.max(SourceImpression.id)))
                    max_impression_id = result.scalar()
                    if max_impression_id:
                        self.last_impression_id = max(0, max_impression_id - 1000)  # Safety margin

                    # Get the last webhit ID with reasonable safety margin
                    result = await session.execute(select(func.max(SourceWebHit.id)))
                    max_webhit_id = result.scalar()
                    if max_webhit_id:
                        self.last_webhit_id = max(0, max_webhit_id - 1000)  # Safety margin

                    logger.info(
                        f"Initialized from source: impression ID {self.last_impression_id}, webhit ID {self.last_webhit_id}")

                    # Persist these values to our state table
                    await self._persist_processing_state(session)
        except Exception as e:
            logger.error(f"Error initializing last IDs: {e}")
            # Continue with default values

    async def _persist_processing_state(self, session: AsyncSession):
        """Update the persistent record of processing state"""
        try:
            # Update impression state
            impression_state = await session.execute(
                select(ProcessingState).where(
                    ProcessingState.entity_type == 'impression'
                )
            )
            imp_state = impression_state.scalar_one_or_none()

            if imp_state:
                imp_state.last_id = self.last_impression_id
                imp_state.updated_at = datetime.now()
            else:
                session.add(ProcessingState(
                    entity_type='impression',
                    last_id=self.last_impression_id,
                    updated_at=datetime.now()
                ))

            # Update webhit state
            webhit_state = await session.execute(
                select(ProcessingState).where(
                    ProcessingState.entity_type == 'webhit'
                )
            )
            wh_state = webhit_state.scalar_one_or_none()

            if wh_state:
                wh_state.last_id = self.last_webhit_id
                wh_state.updated_at = datetime.now()
            else:
                session.add(ProcessingState(
                    entity_type='webhit',
                    last_id=self.last_webhit_id,
                    updated_at=datetime.now()
                ))

            await session.commit()
            logger.debug("Successfully persisted processing state")
        except Exception as e:
            await session.rollback()
            logger.error(f"Error persisting processing state: {e}")

    async def start(self, interval_seconds: int = 60):
        """Begin the collection cycle with sophisticated timing"""
        if self.running:
            logger.warning("Collection service is already running")
            return

        self.running = True
        await self.initialize()

        try:
            while self.running:
                await self.collect_and_queue()
                logger.info(f"Collection cycle complete, waiting {interval_seconds} seconds until next cycle")
                await asyncio.sleep(interval_seconds)
        finally:
            self.running = False

    async def stop(self):
        """Gracefully terminate collection cycles"""
        logger.info("Stopping collection service")
        self.running = False

    async def collect_and_queue(self):
        """Collects new records and queues them with elegant efficiency"""
        if not self._initialized:
            await self.initialize()

        try:
            source_db = await self.db_manager.try_async_source_connections(['TVBVODDB1', 'TVBVODDB2'])
        except Exception as e:
            logger.error(f"Error connecting to source databases: {e}")
            return
        if not source_db:
            logger.error("No source database available. Skipping collection cycle.")
            return

        now = datetime.now()

        try:
            # Process the three collection tasks concurrently
            await asyncio.gather(
                self._process_impressions(source_db),
                self._process_webhits(source_db),
                self._process_pings(source_db)
            )

            # Persist our current state
            async with self.db_manager.async_session_scope('TVFACTORY') as session:
                await self._persist_processing_state(session)

            self.last_run = now
            logger.info(f"Collection cycle completed at {now.isoformat()}")
        except Exception as e:
            logger.error(f"Error during collection cycle: {e}")

    async def _process_impressions(self, source_db: str):
        """Process impressions in batches using ID-based pagination"""
        total_processed = 0
        current_max_id = self.last_impression_id

        try:
            async with self.db_manager.async_session_scope(source_db) as session:
                while True:
                    # Query for the next batch of impressions
                    with timer.time("db_fetch_impressions"):
                        result = await session.execute(
                            select(SourceImpression).where(
                                SourceImpression.id > current_max_id
                            ).order_by(SourceImpression.id).limit(BATCH_SIZE)
                        )

                    impressions = result.scalars().all()

                    if not impressions:
                        break

                    # Process each impression in the batch
                    for impression in impressions:
                        message = {
                            'id': impression.id,
                            'timestmp': impression.timestmp.isoformat(),
                            'client_id': impression.client_id,
                            'booking_id': impression.booking_id,
                            'creative_id': impression.creative_id,
                            'ipaddress': impression.ipaddress,
                            'useragent': impression.useragent
                        }

                        # Publish to RabbitMQ
                        with timer.time("impressions_publish"):
                            await self.rabbitmq_client.publish(
                                exchange='',
                                routing_key='impressions_queue',
                                message=message
                            )

                        # Update the current max ID
                        current_max_id = max(current_max_id, impression.id)

                    total_processed += len(impressions)

                    # Log progress for substantial batches
                    if len(impressions) == BATCH_SIZE:
                        logger.debug(f"Processed {len(impressions)} impressions, continuing from ID {current_max_id}")

                    # If we got fewer records than the batch size, we're done
                    if len(impressions) < BATCH_SIZE:
                        break

            # Update the last processed ID for future runs
            self.last_impression_id = current_max_id
            logger.info(f"Processed {total_processed} impressions up to ID {current_max_id}")
        except Exception as e:
            logger.error(f"Error processing impressions: {e}")
            raise
        timer.tick()

    async def _process_webhits(self, source_db: str):
        """Process webhits in batches using ID-based pagination"""
        total_processed = 0
        current_max_id = self.last_webhit_id

        try:
            async with self.db_manager.async_session_scope(source_db) as session:
                while True:
                    # Query for the next batch of webhits
                    with timer.time("db_fetch_webhits"):
                        result = await session.execute(
                            select(SourceWebHit).where(
                                SourceWebHit.id > current_max_id
                            ).order_by(SourceWebHit.id).limit(BATCH_SIZE)
                        )

                    webhits = result.scalars().all()

                    if not webhits:
                        break

                    # Process each webhit in the batch
                    for webhit in webhits:
                        message = {
                            'id': webhit.id,
                            'timestmp': webhit.timestmp.isoformat(),
                            'client_id': webhit.client_id,
                            'site_id': webhit.site_id,
                            'ipaddress': webhit.ipaddress,
                            'impression_id': webhit.impression_id
                        }

                        # Publish to RabbitMQ
                        with timer.time("webhits_publish"):
                            await self.rabbitmq_client.publish(
                                exchange='',
                                routing_key='webhits_queue',
                                message=message
                            )

                        # Update the current max ID
                        current_max_id = max(current_max_id, webhit.id)

                    total_processed += len(webhits)

                    # Log progress for substantial batches
                    if len(webhits) == BATCH_SIZE:
                        logger.debug(f"Processed {len(webhits)} webhits, continuing from ID {current_max_id}")

                    # If we got fewer records than the batch size, we're done
                    if len(webhits) < BATCH_SIZE:
                        break

            # Update the last processed ID for future runs
            self.last_webhit_id = current_max_id
            logger.info(f"Processed {total_processed} webhits up to ID {current_max_id}")
        except Exception as e:
            logger.error(f"Error processing webhits: {e}")
            raise

    async def _process_pings(self, source_db: str):
        """Process site pings with proper handling"""
        total_processed = 0
        processed_date = self.last_run.date()

        try:
            async with self.db_manager.async_session_scope(source_db) as session:
                while True:
                    # Query for the next batch of pings
                    result = await session.execute(
                        select(SourceLastSiteResponse).where(
                            SourceLastSiteResponse.date >= processed_date
                        ).order_by(SourceLastSiteResponse.date).limit(BATCH_SIZE)
                    )
                    pings = result.scalars().all()

                    if not pings:
                        break

                    # Process this batch of pings
                    async with self.db_manager.async_session_scope('TVFACTORY') as factory_session:
                        for ping in pings:
                            await self._process_single_ping(factory_session, ping)

                    # Update the date for pagination
                    if pings:
                        processed_date = max(ping.date for ping in pings)

                    total_processed += len(pings)

                    # If we got fewer records than the batch size, we're done
                    if len(pings) < BATCH_SIZE:
                        break

            logger.info(f"Processed {total_processed} pings")
        except Exception as e:
            logger.error(f"Error processing pings: {e}")
            raise

    async def _process_single_ping(self, factory_session: AsyncSession, ping: SourceLastSiteResponse):
        """Process a single ping record with proper error handling"""
        try:
            # Check if the record exists
            result = await factory_session.execute(
                select(TargetLastSiteResponse).where(
                    TargetLastSiteResponse.date == ping.date,
                    TargetLastSiteResponse.client_id == ping.client_id,
                    TargetLastSiteResponse.sitetag_id == ping.sitetag_id
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                # Update existing record
                existing.hits = ping.hits
            else:
                # Insert new record
                new_record = TargetLastSiteResponse(
                    date=ping.date,
                    client_id=ping.client_id,
                    sitetag_id=ping.sitetag_id,
                    hits=ping.hits
                )
                factory_session.add(new_record)

            # Commit each change individually for precise error control
            await factory_session.commit()

        except Exception as e:
            await factory_session.rollback()
            logger.error(f"Error handling ping data for client {ping.client_id}, sitetag {ping.sitetag_id}: {e}")
