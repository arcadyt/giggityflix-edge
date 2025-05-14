import json
from unittest.mock import patch, MagicMock

import pytest

from src.config import config
from src.kafka.producer import KafkaProducer


@pytest.mark.asyncio
class TestKafkaProducer:

    async def test_publish_peer_lifecycle_event(self, test_data):
        """Test publishing a peer lifecycle event."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            # Execute
            await producer.publish_peer_lifecycle_event(
                peer_id=test_data["peer_id"],
                event_type="connected"
            )

            # Verify
            mock_producer.produce.assert_called_once()
            args, kwargs = mock_producer.produce.call_args

            # Check topic
            assert args[0] == config.kafka.peer_lifecycle_events_topic

            # Check message content
            message_bytes = kwargs["value"]
            message = json.loads(message_bytes.decode('utf-8'))
            assert message["peer_id"] == test_data["peer_id"]
            assert message["edge_id"] == config.edge.edge_id
            assert message["event_type"] == "connected"
            assert "timestamp" in message

            # Check callback was provided
            assert callable(kwargs["callback"])

            # Verify flush was called
            mock_producer.flush.assert_called_once()

    async def test_publish_peer_catalog_update(self, test_data):
        """Test publishing a peer catalog update."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            catalog_ids = [test_data["catalog_uuid"], "another-catalog-id"]

            # Execute
            await producer.publish_peer_catalog_update(
                peer_id=test_data["peer_id"],
                catalog_ids=catalog_ids
            )

            # Verify
            mock_producer.produce.assert_called_once()
            args, kwargs = mock_producer.produce.call_args

            # Check topic
            assert args[0] == config.kafka.peer_catalog_updates_topic

            # Check message content
            message_bytes = kwargs["value"]
            message = json.loads(message_bytes.decode('utf-8'))
            assert message["peer_id"] == test_data["peer_id"]
            assert message["edge_id"] == config.edge.edge_id
            assert message["catalog_ids"] == catalog_ids
            assert message["is_full_update"] is True
            assert "timestamp" in message

    async def test_publish_file_delete_response(self, test_data):
        """Test publishing a file delete response."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            # Execute
            await producer.publish_file_delete_response(
                peer_id=test_data["peer_id"],
                catalog_uuid=test_data["catalog_uuid"],
                success=True,
                error_message=None
            )

            # Verify
            mock_producer.produce.assert_called_once()
            args, kwargs = mock_producer.produce.call_args

            # Check topic
            assert args[0] == config.kafka.file_delete_responses_topic

            # Check message content
            message_bytes = kwargs["value"]
            message = json.loads(message_bytes.decode('utf-8'))
            assert message["peer_id"] == test_data["peer_id"]
            assert message["edge_id"] == config.edge.edge_id
            assert message["catalog_uuid"] == test_data["catalog_uuid"]
            assert message["success"] is True
            assert message["error_message"] == ""
            assert "timestamp" in message

    async def test_publish_file_hash_response(self, test_data):
        """Test publishing a file hash response."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            hashes = {"md5": "abc123", "sha256": "def456"}

            # Execute
            await producer.publish_file_hash_response(
                peer_id=test_data["peer_id"],
                catalog_uuid=test_data["catalog_uuid"],
                hashes=hashes,
                error_message=None
            )

            # Verify
            mock_producer.produce.assert_called_once()
            args, kwargs = mock_producer.produce.call_args

            # Check topic
            assert args[0] == config.kafka.file_hash_responses_topic

            # Check message content
            message_bytes = kwargs["value"]
            message = json.loads(message_bytes.decode('utf-8'))
            assert message["peer_id"] == test_data["peer_id"]
            assert message["edge_id"] == config.edge.edge_id
            assert message["catalog_uuid"] == test_data["catalog_uuid"]
            assert message["hashes"] == hashes
            assert message["error_message"] == ""
            assert "timestamp" in message

    async def test_publish_deadletter(self, test_data):
        """Test publishing a message to the deadletter queue."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            original_message = {"key": "value"}
            reason = "Test reason"

            # Execute
            await producer.publish_deadletter(
                peer_id=test_data["peer_id"],
                original_message=original_message,
                reason=reason
            )

            # Verify
            mock_producer.produce.assert_called_once()
            args, kwargs = mock_producer.produce.call_args

            # Check topic format
            expected_topic = config.kafka.deadletter_topic_pattern.format(config.edge.edge_id)
            assert args[0] == expected_topic

            # Check message content
            message_bytes = kwargs["value"]
            message = json.loads(message_bytes.decode('utf-8'))
            assert message["peer_id"] == test_data["peer_id"]
            assert message["edge_id"] == config.edge.edge_id
            assert message["original_message"] == original_message
            assert message["reason"] == reason
            assert "timestamp" in message

    def test_delivery_report_success(self):
        """Test the delivery report callback with successful delivery."""
        # Setup
        producer = KafkaProducer()

        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0

        # Execute
        with patch('src.kafka.producer.logger') as mock_logger:
            producer._delivery_report(None, msg)

            # Verify
            mock_logger.debug.assert_called_once()
            assert "Message delivered" in mock_logger.debug.call_args[0][0]
            assert "test-topic" in mock_logger.debug.call_args[0][0]

    def test_delivery_report_error(self):
        """Test the delivery report callback with delivery error."""
        # Setup
        producer = KafkaProducer()
        error = "Test error"

        # Execute
        with patch('src.kafka.producer.logger') as mock_logger:
            producer._delivery_report(error, None)

            # Verify
            mock_logger.error.assert_called_once()
            assert "Message delivery failed" in mock_logger.error.call_args[0][0]
            assert "Test error" in mock_logger.error.call_args[0][0]

    async def test_publish_message_exception(self, test_data):
        """Test error handling in _publish_message."""
        # Setup
        with patch('src.kafka.producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer.produce.side_effect = Exception("Test exception")
            mock_producer_class.return_value = mock_producer

            producer = KafkaProducer()

            # Execute
            with patch('src.kafka.producer.logger') as mock_logger:
                producer._publish_message("test-topic", {"key": "value"})

                # Verify
                mock_logger.error.assert_called_once()
                assert "Failed to publish message to test-topic" in mock_logger.error.call_args[0][0]
                assert "Test exception" in mock_logger.error.call_args[0][0]
