import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from services.webhit_consumer import WebhitConsumerService
# from config.settings import settings # May be needed for settings.ONE_DAY
# settings.ONE_DAY is used, so we will patch it.

class TestWebhitConsumerServiceOptimizations(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Base setup for each test
        self.consumer = WebhitConsumerService(run_once=True)
        
        # Mock dependencies that are not directly under test
        self.consumer.redis = AsyncMock()
        self.consumer.db = AsyncMock() # If db interactions happen before/after sismember
        self.consumer.rabbit = AsyncMock() # If rabbit interactions happen

        # Default return values for mocks to represent a "happy path" initially
        # For Redis imp_key check
        self.consumer.redis.get.return_value = "123" # Simulate impression ID found
        
        # Mock Redis pipeline behavior
        # pipeline() should return an object that itself has sadd, expire, and execute methods.
        # Making the redis mock itself the pipeline mock for simplicity here,
        # as long as we ensure execute is an AsyncMock.
        mock_pipeline = AsyncMock()
        mock_pipeline.sadd.return_value = mock_pipeline # chainable
        mock_pipeline.expire.return_value = mock_pipeline # chainable
        mock_pipeline.execute = AsyncMock(return_value=[1, 1]) # Simulate success for sadd, expire
        self.consumer.redis.pipeline.return_value = mock_pipeline


        # Mock rabbit publish
        self.consumer.rabbit.publish = AsyncMock()
        
        # Mock global:next_webhit_id increment
        self.consumer.redis.incr.return_value = 456 # Simulate a new webhit ID

    @patch('services.webhit_consumer.settings') # Patch settings if it's used by the method
    async def test_sismember_called_and_site_not_seen(self, mock_settings):
        mock_settings.ONE_DAY = 86400 # Example: if settings.ONE_DAY is used by the method

        # Simulate site_id NOT being in the Redis set (sismember returns 0)
        self.consumer.redis.sismember.return_value = 0  # Not a member

        client_id = 100
        ip = "127.0.0.1"
        site_id = 1
        timestamp = datetime.now()
        
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}"

        result = await self.consumer._process_webhit_find_impression(client_id, ip, site_id, timestamp)

        self.assertTrue(result) # Should successfully process as site is not a duplicate
        self.consumer.redis.sismember.assert_called_once_with(dedupe_key, str(site_id))
        
        # Verify that the new site_id is added to the set via pipeline
        pipeline_mock = self.consumer.redis.pipeline.return_value
        pipeline_mock.sadd.assert_called_once_with(dedupe_key, str(site_id))
        pipeline_mock.expire.assert_called_once_with(dedupe_key, mock_settings.ONE_DAY)
        pipeline_mock.execute.assert_called_once()
        
        # Verify it was published
        self.consumer.rabbit.publish.assert_called_once()
        # Add more assertions on the payload if necessary
        # Example: Check the first argument of the first call to publish (the message payload)
        args, _ = self.consumer.rabbit.publish.call_args
        # args[0] is exchange, args[1] is routing_key, args[2] is message
        # Assuming publish is called like publish(exchange="", routing_key="...", message=payload)
        # The actual call might be self.rabbit.publish(message=payload, exchange="", routing_key="...")
        # So, checking kwargs might be safer if the order is not fixed.
        # For now, let's assume the provided structure in webhit_consumer.py:
        # await self.rabbit.publish(exchange="", routing_key="resolved_webhits_queue", message=payload)
        
        # Check the message payload
        published_payload = self.consumer.rabbit.publish.call_args[1]['message'] # Accessing via kwargs
        self.assertEqual(published_payload['id'], 456) # From redis.incr mock
        self.assertEqual(published_payload['impression_id'], 123) # From redis.get mock
        self.assertEqual(published_payload['client_id'], client_id)
        self.assertEqual(published_payload['site_id'], site_id)
        self.assertEqual(published_payload['ipaddress'], ip)
        self.assertEqual(published_payload['timestmp'], timestamp.isoformat())


    @patch('services.webhit_consumer.settings')
    async def test_sismember_called_and_site_seen(self, mock_settings):
        mock_settings.ONE_DAY = 86400

        # Simulate site_id IS in the Redis set (sismember returns 1)
        self.consumer.redis.sismember.return_value = 1  # Is a member (duplicate)

        client_id = 100
        ip = "127.0.0.1"
        site_id = 1
        timestamp = datetime.now()
        
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}"

        result = await self.consumer._process_webhit_find_impression(client_id, ip, site_id, timestamp)

        self.assertFalse(result) # Should return False as it's a duplicate
        self.consumer.redis.sismember.assert_called_once_with(dedupe_key, str(site_id))
        
        # Verify that sadd and publish are NOT called
        pipeline_mock = self.consumer.redis.pipeline.return_value
        pipeline_mock.sadd.assert_not_called()
        pipeline_mock.execute.assert_not_called() # If sadd is not called, execute on pipeline won't be either for this path
        self.consumer.rabbit.publish.assert_not_called()

    # Test for DB fallback when impression_id is not in Redis
    @patch('services.webhit_consumer.settings')
    async def test_db_fallback_sismember_called_and_site_not_seen(self, mock_settings):
        mock_settings.ONE_DAY = 86400

        # Simulate impression ID NOT found in Redis
        self.consumer.redis.get.return_value = None 
        
        # Mock DB calls for impression lookup
        mock_db_session = AsyncMock()
        mock_execute_result_impression = MagicMock()
        # Simulate DB returns an impression ID
        mock_execute_result_impression.scalars.return_value.first.return_value = 789 
        
        # Mock DB calls for webhit dedupe check (simulate no existing webhit)
        mock_execute_result_webhit_dedupe = MagicMock()
        mock_execute_result_webhit_dedupe.scalars.return_value.first.return_value = None # No duplicate webhit in DB

        # Set up side_effect for db.execute to handle multiple calls:
        # 1st call: impression lookup
        # 2nd call: webhit dedupe check (which should happen if impression is found)
        mock_db_session.execute.side_effect = [
            mock_execute_result_impression, 
            mock_execute_result_webhit_dedupe
        ]
        
        # Patch the db.async_session_scope to return our mock session
        # The context manager __aenter__ should return the mock_db_session
        mock_async_scope = MagicMock()
        mock_async_scope.__aenter__.return_value = mock_db_session
        self.consumer.db.async_session_scope.return_value = mock_async_scope
        
        # Simulate site_id NOT being in the Redis set (sismember returns 0)
        # This sismember check is *not* expected to be called in this DB fallback path
        # because the dedupe_key is based on client_id:ip, and if imp_id was found in DB,
        # the Redis dedupe check for site_id is skipped.
        # The logic is: if imp_id from redis, check redis for site_id.
        # If imp_id from DB, check DB for site_id.
        # So, sismember should NOT be called here.
        self.consumer.redis.sismember.return_value = 0 # Default, but shouldn't be called.

        client_id = 200 # Different client_id for this test
        ip = "192.168.1.1"
        site_id = 2
        timestamp = datetime.now()
        
        # This dedupe_key is for the Redis path, which we are bypassing for imp_id lookup
        # but it will be used for SADD if the DB path succeeds.
        dedupe_key = f"dedupe:webhit:{client_id}:{ip}" 

        result = await self.consumer._process_webhit_find_impression(client_id, ip, site_id, timestamp)

        self.assertTrue(result) # Should successfully process
        
        # Verify Redis get was called (and returned None)
        self.consumer.redis.get.assert_called_once_with(f"imp:{client_id}:{ip}")
        
        # Verify DB calls
        self.assertEqual(mock_db_session.execute.call_count, 2) # Impression lookup and WebHit dedupe

        # sismember should NOT have been called because impression was found via DB
        self.consumer.redis.sismember.assert_not_called() 
        
        # Verify that the new site_id is added to the Redis set (even after DB path)
        pipeline_mock = self.consumer.redis.pipeline.return_value
        pipeline_mock.sadd.assert_called_once_with(dedupe_key, str(site_id))
        pipeline_mock.expire.assert_called_once_with(dedupe_key, mock_settings.ONE_DAY)
        pipeline_mock.execute.assert_called_once()
        
        # Verify it was published
        self.consumer.rabbit.publish.assert_called_once()
        published_payload = self.consumer.rabbit.publish.call_args[1]['message']
        self.assertEqual(published_payload['impression_id'], 789) # From DB mock
        self.assertEqual(published_payload['client_id'], client_id)


if __name__ == '__main__':
    unittest.main()
