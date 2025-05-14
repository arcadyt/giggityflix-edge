import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.message_handler import MessageHandler
from src.grpc.generated import peer_edge_pb2 as pb2


@pytest.mark.asyncio
class TestMessageHandler:
    
    async def test_handle_peer_registration(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a peer registration request."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        catalog_uuids = ["catalog-1", "catalog-2"]
        request_id = test_data["request_id"]
        
        # Execute
        response = await handler.handle_peer_registration(
            test_data["peer_id"], catalog_uuids, request_id
        )
        
        # Verify
        mock_kafka_producer.publish_peer_catalog_update.assert_called_once_with(
            peer_id=test_data["peer_id"],
            catalog_ids=catalog_uuids
        )
        
        assert response.request_id == request_id
        assert hasattr(response, 'registration_response')
        assert response.registration_response.peer_name == test_data["peer_id"]
        assert response.registration_response.success is True
    
    async def test_handle_file_delete_response(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a file delete response."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        
        # Create mock file delete response
        file_delete_response = MagicMock()
        file_delete_response.catalog_uuid = test_data["catalog_uuid"]
        file_delete_response.success = True
        file_delete_response.error_message = ""
        
        # Execute
        await handler.handle_file_delete_response(test_data["peer_id"], file_delete_response)
        
        # Verify
        mock_kafka_producer.publish_file_delete_response.assert_called_once_with(
            peer_id=test_data["peer_id"],
            catalog_uuid=test_data["catalog_uuid"],
            success=True,
            error_message=""
        )
    
    async def test_handle_file_hash_response(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a file hash response."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        
        # Create mock file hash response
        file_hash_response = MagicMock()
        file_hash_response.catalog_uuid = test_data["catalog_uuid"]
        file_hash_response.hashes = {"md5": "abc123", "sha256": "def456"}
        file_hash_response.error_message = ""
        
        # Execute
        await handler.handle_file_hash_response(test_data["peer_id"], file_hash_response)
        
        # Verify
        mock_kafka_producer.publish_file_hash_response.assert_called_once_with(
            peer_id=test_data["peer_id"],
            catalog_uuid=test_data["catalog_uuid"],
            hashes={"md5": "abc123", "sha256": "def456"},
            error_message=""
        )
    
    async def test_handle_batch_file_offer(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a batch file offer."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        
        # Create mock batch file offer request
        batch_file_offer_request = MagicMock()
        batch_file_offer_request.category_type = "video"
        
        # Mock files in request
        mock_file1 = MagicMock()
        mock_file1.peer_luid = "local-file-1"
        mock_file2 = MagicMock()
        mock_file2.peer_luid = "local-file-2"
        batch_file_offer_request.files = [mock_file1, mock_file2]
        
        # Execute
        with patch('uuid.uuid4', side_effect=["uuid-1", "uuid-2"]):
            await handler.handle_batch_file_offer(test_data["peer_id"], batch_file_offer_request)
        
        # Verify
        mock_kafka_producer.publish_peer_catalog_update.assert_called_once()
        peer_id_arg = mock_kafka_producer.publish_peer_catalog_update.call_args[1]['peer_id']
        catalog_ids_arg = mock_kafka_producer.publish_peer_catalog_update.call_args[1]['catalog_ids']
        
        assert peer_id_arg == test_data["peer_id"]
        assert len(catalog_ids_arg) == 2
        assert catalog_ids_arg[0] == "uuid-1"
        assert catalog_ids_arg[1] == "uuid-2"
    
    async def test_handle_screenshot_capture_response_success(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a successful screenshot capture response."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        
        # Create mock screenshot capture response
        screenshot_response = MagicMock()
        screenshot_response.catalog_uuid = test_data["catalog_uuid"]
        screenshot_response.HasField = MagicMock(return_value=False)  # No error message
        
        # Execute
        with patch('src.message_handler.logger') as mock_logger:
            await handler.handle_screenshot_capture_response(test_data["peer_id"], screenshot_response)
            
            # Verify
            mock_logger.info.assert_called_once()
            assert "acknowledged" in mock_logger.info.call_args[0][0]
    
    async def test_handle_screenshot_capture_response_error(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a failed screenshot capture response."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        
        # Create mock screenshot capture response with error
        screenshot_response = MagicMock()
        screenshot_response.catalog_uuid = test_data["catalog_uuid"]
        screenshot_response.HasField = MagicMock(return_value=True)  # Has error message
        screenshot_response.error_message = "Failed to capture screenshot"
        
        # Execute
        with patch('src.message_handler.logger') as mock_logger:
            await handler.handle_screenshot_capture_response(test_data["peer_id"], screenshot_response)
            
            # Verify
            mock_logger.error.assert_called_once()
            assert "Error capturing screenshot" in mock_logger.error.call_args[0][0]
    
    async def test_handle_edge_command_peer_not_connected(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a command for a peer that's not connected."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = False
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "file_delete",
            "request_id": test_data["request_id"],
            "command_data": {
                "catalog_uuids": [test_data["catalog_uuid"]]
            }
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.is_peer_connected.assert_called_once_with(test_data["peer_id"])
        mock_kafka_producer.publish_deadletter.assert_called_once_with(
            peer_id=test_data["peer_id"],
            original_message=command,
            reason="Peer not connected"
        )
    
    async def test_handle_edge_command_unknown_type(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a command with unknown type."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = True
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "unknown_command",
            "request_id": test_data["request_id"],
            "command_data": {}
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.is_peer_connected.assert_called_once_with(test_data["peer_id"])
        mock_kafka_producer.publish_deadletter.assert_called_once_with(
            peer_id=test_data["peer_id"],
            original_message=command,
            reason="Unknown command type: unknown_command"
        )
    
    async def test_handle_edge_command_file_delete(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a file delete command."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = True
        mock_stream_manager.send_message_to_peer.return_value = True
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "file_delete",
            "request_id": test_data["request_id"],
            "command_data": {
                "catalog_uuids": [test_data["catalog_uuid"]]
            }
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.is_peer_connected.assert_called_once_with(test_data["peer_id"])
        mock_stream_manager.send_message_to_peer.assert_called_once()
        
        # Check message content
        message_arg = mock_stream_manager.send_message_to_peer.call_args[1]['message']
        assert message_arg.request_id == test_data["request_id"]
        assert hasattr(message_arg, 'file_delete_request')
        assert test_data["catalog_uuid"] in message_arg.file_delete_request.catalog_uuids
    
    async def test_handle_edge_command_file_hash(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a file hash command."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = True
        mock_stream_manager.send_message_to_peer.return_value = True
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "file_hash",
            "request_id": test_data["request_id"],
            "command_data": {
                "catalog_uuid": test_data["catalog_uuid"],
                "hash_types": ["md5", "sha256"]
            }
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.is_peer_connected.assert_called_once_with(test_data["peer_id"])
        mock_stream_manager.send_message_to_peer.assert_called_once()
        
        # Check message content
        message_arg = mock_stream_manager.send_message_to_peer.call_args[1]['message']
        assert message_arg.request_id == test_data["request_id"]
        assert hasattr(message_arg, 'file_hash_request')
        assert message_arg.file_hash_request.catalog_uuid == test_data["catalog_uuid"]
        assert list(message_arg.file_hash_request.hash_types) == ["md5", "sha256"]
    
    async def test_handle_edge_command_send_failure(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a command where sending to peer fails."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = True
        mock_stream_manager.send_message_to_peer.return_value = False
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "file_delete",
            "request_id": test_data["request_id"],
            "command_data": {
                "catalog_uuids": [test_data["catalog_uuid"]]
            }
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.send_message_to_peer.assert_called_once()
        mock_kafka_producer.publish_deadletter.assert_called_once_with(
            peer_id=test_data["peer_id"],
            original_message=command["command_data"],
            reason="Failed to send message to peer"
        )
    
    async def test_handle_edge_command_exception(self, mock_stream_manager, mock_kafka_producer, test_data):
        """Test handling a command that raises an exception."""
        # Setup
        handler = MessageHandler(mock_stream_manager, mock_kafka_producer)
        mock_stream_manager.is_peer_connected.return_value = True
        mock_stream_manager.send_message_to_peer.side_effect = Exception("Test exception")
        
        command = {
            "peer_id": test_data["peer_id"],
            "command_type": "file_delete",
            "request_id": test_data["request_id"],
            "command_data": {
                "catalog_uuids": [test_data["catalog_uuid"]]
            }
        }
        
        # Execute
        await handler.handle_edge_command(command)
        
        # Verify
        mock_stream_manager.send_message_to_peer.assert_called_once()
        mock_kafka_producer.publish_deadletter.assert_called_once_with(
            peer_id=test_data["peer_id"],
            original_message=command,
            reason="Error: Test exception"
        )
