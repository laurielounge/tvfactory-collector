# services/collection_service.py
import json
from datetime import datetime, timedelta

import pika

from core.logger import logger
from infrastructure.database import DatabaseManager
from models.impression import SourceImpression
from models.ping import SourceLastSiteResponse
from models.webhit import SourceWebHit

BATCH_SIZE = 10


class CollectionService:
    """Orchestrates data collection with refined precision"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.last_run = datetime.now() - timedelta(minutes=15)

        # Connect to RabbitMQ with quiet confidence
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.channel = connection.channel()

        # Declare queues with necessary persistence
        self.channel.queue_declare(queue='impressions_queue', durable=True)
        self.channel.queue_declare(queue='webhits_queue', durable=True)
        self.channel.queue_declare(queue='pings_queue', durable=True)

    def collect_and_queue(self):
        """Collects new records and queues them with elegant efficiency"""
        source_db = self.db_manager.try_source_connections(['tvbvoddb1', 'tvbvoddb2'])
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
                            'site_id': webhit.booking_id,
                            'ipaddress': webhit.ipaddress,
                            'impression_id': webhit.impression_id
                        }),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )

                pings = session.query(SourceLastSiteResponse).filter(
                    SourceLastSiteResponse.date >= self.last_run.date()
                ).limit(5000).all()

                # Queue impressions with appropriate serialization
                for ping in pings:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key='pings_queue',
                        body=json.dumps({
                            'date': ping.date,
                            'client_id': ping.client_id,
                            'site_tag_id': ping.site_tag_id,
                            'hits': webhit.hits
                        }),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )

        self.last_run = now
        logger.info(f"Collection cycle completed. Queued {len(impressions)} impressions.")
