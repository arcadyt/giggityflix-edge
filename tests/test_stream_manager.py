import asyncio
from unittest.mock import MagicMock, AsyncMock

import pytest

from src.stream_manager import StreamManager


@pytest.mark.asyncio
class TestStreamManager:

    async def test_register_peer(self, mock_kafka_producer, test_data, mock_grpc_context):
        """Test registering a new peer."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Mock the _schedule_task method to avoid asyncio issues
        stream_manager._schedule_task = MagicMock()

        # Register a peer
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Verify peer was registered
        assert test_data["peer_id"] in stream_manager.peer_streams
        assert stream_manager.peer_streams[test_data["peer_id"]] == mock_grpc_context

        # Verify _schedule_task was called with the correct coroutine
        stream_manager._schedule_task.assert_called_once()
        # We can't directly check the coroutine object, but we can verify it was called

    async def test_unregister_peer(self, mock_kafka_producer, test_data, mock_grpc_context):
        """Test unregistering a peer."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Register a peer first
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Unregister the peer
        await stream_manager.unregister_peer(test_data["peer_id"])

        # Verify peer was unregistered
        assert test_data["peer_id"] not in stream_manager.peer_streams

        # Verify Kafka event was published
        mock_kafka_producer.publish_peer_lifecycle_event.assert_called_with(
            peer_id=test_data["peer_id"],
            event_type="disconnected"
        )

    async def test_send_message_to_peer_success(self, mock_kafka_producer, test_data, mock_grpc_context,
                                                mock_edge_message):
        """Test sending a message to a peer successfully."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Register a peer first
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Send a message
        mock_grpc_context.write = AsyncMock(return_value=None)
        success = await stream_manager.send_message_to_peer(test_data["peer_id"], mock_edge_message)

        # Verify message was sent
        assert success is True
        mock_grpc_context.write.assert_called_once_with(mock_edge_message)

    async def test_send_message_to_peer_not_connected(self, mock_kafka_producer, test_data, mock_edge_message):
        """Test sending a message to a peer that is not connected."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Send a message to a non-existent peer
        success = await stream_manager.send_message_to_peer(test_data["peer_id"], mock_edge_message)

        # Verify message was not sent
        assert success is False

    async def test_send_message_to_peer_error(self, mock_kafka_producer, test_data, mock_grpc_context,
                                              mock_edge_message):
        """Test sending a message to a peer that raises an error."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Register a peer first
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Make write raise an exception
        mock_grpc_context.write = AsyncMock(side_effect=Exception("Test error"))

        # Send a message
        success = await stream_manager.send_message_to_peer(test_data["peer_id"], mock_edge_message)

        # Verify message was not sent
        assert success is False

    async def test_send_message_to_peer_with_retry(self, mock_kafka_producer, test_data, mock_grpc_context,
                                                   mock_edge_message):
        """Test sending a message to a peer with retry callback."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Register a peer first
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Make write raise an exception
        mock_grpc_context.write = AsyncMock(side_effect=Exception("Test error"))

        # Create a mock retry callback
        mock_retry_callback = AsyncMock()

        # Send a message with retry callback
        success = await stream_manager.send_message_to_peer(
            test_data["peer_id"],
            mock_edge_message,
            retry_callback=mock_retry_callback
        )

        # Verify message was not sent
        assert success is False

        # Verify retry callback was scheduled
        await asyncio.sleep(0.1)  # Give time for the async task to run
        mock_retry_callback.assert_called_once_with(test_data["peer_id"], mock_edge_message)

    def test_is_peer_connected(self, mock_kafka_producer, test_data, mock_grpc_context):
        """Test checking if a peer is connected."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Check if a non-existent peer is connected
        assert stream_manager.is_peer_connected(test_data["peer_id"]) is False

        # Mock the _schedule_task method to avoid asyncio issues
        stream_manager._schedule_task = MagicMock()

        # Register a peer
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Check if the peer is connected
        assert stream_manager.is_peer_connected(test_data["peer_id"]) is True

    def test_get_connected_peers(self, mock_kafka_producer, test_data, mock_grpc_context):
        """Test getting all connected peers."""
        # Create StreamManager with mock KafkaProducer
        stream_manager = StreamManager(mock_kafka_producer)

        # Check with no peers
        assert stream_manager.get_connected_peers() == []

        # Mock the _schedule_task method to avoid asyncio issues
        stream_manager._schedule_task = MagicMock()

        # Register a peer
        stream_manager.register_peer(test_data["peer_id"], mock_grpc_context)

        # Check with one peer
        assert stream_manager.get_connected_peers() == [test_data["peer_id"]]

        # Register another peer
        stream_manager.register_peer("another-peer", mock_grpc_context)

        # Check with multiple peers
        peers = stream_manager.get_connected_peers()
        assert len(peers) == 2
        assert test_data["peer_id"] in peers
        assert "another-peer" in peers
