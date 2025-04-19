# services/collection_service.py
import json
from datetime import datetime, timedelta

import pika

from config.config import settings
from core.logger import logger
from infrastructure.database import DatabaseManager
from models.impression import SourceImpression
from models.ping import SourceLastSiteResponse, TargetLastSiteResponse
from models.webhit import SourceWebHit

BATCH_SIZE = 10


class CollectionService:
    """Orchestrates data collection with refined precision"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.last_run = datetime.now() - timedelta(minutes=15)

        # Connect to RabbitMQ with quiet confidence
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=settings.RABBITMQ_HOST,
                virtual_host=settings.RABBITMQ_VHOST,
                credentials=pika.PlainCredentials(settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD)
            )
        )

        self.channel = connection.channel()

        # Declare queues with necessary persistence
        self.channel.queue_declare(queue='impressions_queue', durable=True)
        self.channel.queue_declare(queue='webhits_queue', durable=True)
        self.channel.queue_declare(queue='pings_queue', durable=True)

    def collect_and_queue(self):
        """Collects new records and queues them with elegant efficiency"""
        try:
            source_db = self.db_manager.try_source_connections(['TVBVODDB1', 'TVBVODDB2'])
        except Exception as e:
            logger.error(f"Error connecting to source databases: {e}")
            return
        if not source_db:
            logger.error("No source database available. Skipping collection cycle.")
            return

        now = datetime.now()

        # Extract impressions using your existing models
        with self.db_manager.session_scope(source_db) as session:
            impressions = session.query(SourceImpression).filter(
                SourceImpression.timestmp > self.last_run,
                SourceImpression.timestmp <= now
            ).limit(BATCH_SIZE).all()

            # Queue impressions with appropriate serialization
            for impression in impressions:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='impressions_queue',
                    body=json.dumps({
                        'id': impression.id,
                        'timestmp': impression.timestmp.isoformat(),
                        'client_id': impression.client_id,
                        'booking_id': impression.booking_id,
                        'creative_id': impression.creative_id,
                        'ipaddress': impression.ipaddress,
                        'useragent': impression.useragent
                    }),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

            webhits = session.query(SourceWebHit).filter(
                SourceWebHit.timestmp > self.last_run,
                SourceWebHit.timestmp <= now
            ).limit(BATCH_SIZE).all()

            # Queue impressions with appropriate serialization
            for webhit in webhits:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='webhits_queue',
                    body=json.dumps({
                        'id': webhit.id,
                        'timestmp': webhit.timestmp.isoformat(),
                        'client_id': webhit.client_id,
                        'site_id': webhit.site_id,
                        'ipaddress': webhit.ipaddress,
                        'impression_id': webhit.impression_id
                    }),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

            pings = session.query(SourceLastSiteResponse).filter(
                SourceLastSiteResponse.date >= self.last_run.date()
            ).limit(BATCH_SIZE).all()

            with self.db_manager.session_scope('TVFACTORY') as factory_session:
                for ping in pings:
                    try:
                        # Check if the record exists
                        existing = factory_session.query(TargetLastSiteResponse).filter_by(
                            date=ping.date,
                            client_id=ping.client_id,
                            sitetag_id=ping.sitetag_id
                        ).first()

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

                        # Commit each change individually to handle errors gracefully
                        factory_session.commit()

                    except Exception as e:
                        factory_session.rollback()
                        logger.error(
                            f"Error handling ping data for client {ping.client_id}, sitetag {ping.sitetag_id}: {e}")

        self.last_run = now
        logger.info(f"Collection cycle completed. Queued {len(impressions)} impressions.")
