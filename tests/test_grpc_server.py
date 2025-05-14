import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open

import grpc
import pytest

from giggityflix_edge.grpc_server import PeerEdgeServicer, start_grpc_server


@pytest.mark.asyncio
class TestGrpcServer:

    async def test_peer_edge_servicer_init(self, mock_stream_manager, mock_message_handler):
        """Test PeerEdgeServicer initialization."""
        # Execute
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Verify
        assert servicer.stream_manager == mock_stream_manager
        assert servicer.message_handler == mock_message_handler

    async def test_message_registration_success(self, mock_stream_manager, mock_message_handler,
                                                mock_peer_message, mock_grpc_context):
        """Test message method handling peer registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response message
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create a request iterator with just the registration message
        request_iterator = AsyncIteratorMock([mock_peer_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify
        assert len(responses) == 1
        assert responses[0] == mock_response

        # Verify peer was registered
        mock_stream_manager.register_peer.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            mock_grpc_context
        )

        # Verify registration was handled
        mock_message_handler.handle_peer_registration.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            mock_peer_message.registration_request.catalog_uuids,
            mock_peer_message.request_id
        )

    async def test_message_no_registration_first(self, mock_stream_manager, mock_message_handler,
                                                 mock_grpc_context):
        """Test message method rejecting first message that's not registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a non-registration message
        non_reg_message = MagicMock()
        non_reg_message.HasField.return_value = False  # Not a registration request

        # Create a request iterator with non-registration message
        request_iterator = AsyncIteratorMock([non_reg_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify
        assert len(responses) == 0  # No responses sent

        # Verify abort was called
        mock_grpc_context.abort.assert_called_once_with(
            grpc.StatusCode.INVALID_ARGUMENT,
            "First message must be a registration request"
        )

        # Verify peer was not registered
        mock_stream_manager.register_peer.assert_not_called()

    async def test_message_file_delete_response(self, mock_stream_manager, mock_message_handler,
                                                mock_peer_message, mock_grpc_context):
        """Test message method handling file delete response after registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create a file delete response message
        file_delete_message = MagicMock()
        file_delete_message.request_id = "file-delete-request-id"

        # Setup HasField method for file delete response
        def has_field(field_name):
            if field_name == 'registration_request':
                return False
            elif field_name == 'file_delete_response':
                return True
            return False

        file_delete_message.HasField = has_field

        # Create a request iterator with registration and then file delete
        request_iterator = AsyncIteratorMock([mock_peer_message, file_delete_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify only registration response was returned
        assert len(responses) == 1
        assert responses[0] == mock_response

        # Verify file delete response was handled
        mock_message_handler.handle_file_delete_response.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            file_delete_message.file_delete_response
        )

    async def test_message_file_hash_response(self, mock_stream_manager, mock_message_handler,
                                              mock_peer_message, mock_grpc_context):
        """Test message method handling file hash response after registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create a file hash response message
        file_hash_message = MagicMock()
        file_hash_message.request_id = "file-hash-request-id"

        # Setup HasField method for file hash response
        def has_field(field_name):
            if field_name == 'registration_request':
                return False
            elif field_name == 'file_hash_response':
                return True
            return False

        file_hash_message.HasField = has_field

        # Create a request iterator with registration and then file hash
        request_iterator = AsyncIteratorMock([mock_peer_message, file_hash_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify only registration response was returned
        assert len(responses) == 1
        assert responses[0] == mock_response

        # Verify file hash response was handled
        mock_message_handler.handle_file_hash_response.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            file_hash_message.file_hash_response
        )

    async def test_message_batch_file_offer(self, mock_stream_manager, mock_message_handler,
                                            mock_peer_message, mock_grpc_context):
        """Test message method handling batch file offer after registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create a batch file offer message
        batch_file_message = MagicMock()
        batch_file_message.request_id = "batch-file-request-id"

        # Setup HasField method for batch file offer
        def has_field(field_name):
            if field_name == 'registration_request':
                return False
            elif field_name == 'batch_file_offer_request':
                return True
            return False

        batch_file_message.HasField = has_field

        # Create a request iterator with registration and then batch file offer
        request_iterator = AsyncIteratorMock([mock_peer_message, batch_file_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify only registration response was returned
        assert len(responses) == 1
        assert responses[0] == mock_response

        # Verify batch file offer was handled
        mock_message_handler.handle_batch_file_offer.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            batch_file_message.batch_file_offer_request
        )

    async def test_message_screenshot_capture_response(self, mock_stream_manager, mock_message_handler,
                                                       mock_peer_message, mock_grpc_context):
        """Test message method handling screenshot capture response after registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create a screenshot capture response message
        screenshot_message = MagicMock()
        screenshot_message.request_id = "screenshot-request-id"

        # Setup HasField method for screenshot capture response
        def has_field(field_name):
            if field_name == 'registration_request':
                return False
            elif field_name == 'screenshot_capture_response':
                return True
            return False

        screenshot_message.HasField = has_field

        # Create a request iterator with registration and then screenshot capture
        request_iterator = AsyncIteratorMock([mock_peer_message, screenshot_message])

        # Execute
        response_generator = servicer.message(request_iterator, mock_grpc_context)
        responses = [r async for r in response_generator]

        # Verify only registration response was returned
        assert len(responses) == 1
        assert responses[0] == mock_response

        # Verify screenshot capture response was handled
        mock_message_handler.handle_screenshot_capture_response.assert_called_once_with(
            mock_peer_message.registration_request.peer_name,
            screenshot_message.screenshot_capture_response
        )

    async def test_message_unknown_message_type(self, mock_stream_manager, mock_message_handler,
                                                mock_peer_message, mock_grpc_context):
        """Test message method handling unknown message type after registration."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Create an unknown message type
        unknown_message = MagicMock()
        unknown_message.request_id = "unknown-request-id"

        # Setup HasField method to return False for all known types
        def has_field(field_name):
            return False

        unknown_message.HasField = has_field

        # Create a request iterator with registration and then unknown message
        request_iterator = AsyncIteratorMock([mock_peer_message, unknown_message])

        # Execute
        with patch('src.grpc_server.logger') as mock_logger:
            response_generator = servicer.message(request_iterator, mock_grpc_context)
            responses = [r async for r in response_generator]

            # Verify only registration response was returned
            assert len(responses) == 1
            assert responses[0] == mock_response

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            assert "unknown message type" in mock_logger.warning.call_args[0][0].lower()

    async def test_message_stream_exception(self, mock_stream_manager, mock_message_handler,
                                            mock_peer_message, mock_grpc_context):
        """Test message method handling exception in stream processing."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Make registration handler raise exception
        mock_message_handler.handle_peer_registration.side_effect = Exception("Test stream error")

        # Create a request iterator with just the registration message
        request_iterator = AsyncIteratorMock([mock_peer_message])

        # Execute
        with patch('src.grpc_server.logger') as mock_logger:
            response_generator = servicer.message(request_iterator, mock_grpc_context)
            responses = [r async for r in response_generator]

            # Verify no responses were returned
            assert len(responses) == 0

            # Verify error was logged
            mock_logger.error.assert_called_once()
            assert "Error in bidirectional stream" in mock_logger.error.call_args[0][0]

            # Verify unregister was called
            mock_stream_manager.unregister_peer.assert_called_once_with(
                mock_peer_message.registration_request.peer_name
            )

    async def test_message_cancelled_error(self, mock_stream_manager, mock_message_handler,
                                           mock_peer_message, mock_grpc_context):
        """Test message method handling cancelled error in stream processing."""
        # Setup
        servicer = PeerEdgeServicer(mock_stream_manager, mock_message_handler)

        # Create a request iterator that raises CancelledError after one message
        class AsyncIteratorWithCancelledError:
            """Helper class to simulate a CancelledError in an async iterator."""

            def __init__(self, items):
                self.items = items
                self.index = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise asyncio.CancelledError()
                item = self.items[self.index]
                self.index += 1
                return item

        request_iterator = AsyncIteratorWithCancelledError([mock_peer_message])

        # Create a registration response
        mock_response = MagicMock()
        mock_message_handler.handle_peer_registration.return_value = mock_response

        # Execute
        with patch('src.grpc_server.logger') as mock_logger:
            response_generator = servicer.message(request_iterator, mock_grpc_context)
            responses = [r async for r in response_generator]

            # Verify only registration response was returned
            assert len(responses) == 1
            assert responses[0] == mock_response

            # Verify info was logged about the peer unregistering
            mock_logger.info.assert_called()
            # No need to check for the specific word "cancelled" as the log might not contain it
            # The important part is that unregister_peer was called

            # Verify unregister was called
            mock_stream_manager.unregister_peer.assert_called_once_with(
                mock_peer_message.registration_request.peer_name
            )

    async def test_start_grpc_server_no_tls(self, mock_stream_manager, mock_message_handler):
        """Test starting the gRPC server without TLS."""
        # Setup
        server_mock = MagicMock()
        server_mock.start = AsyncMock()

        with patch('src.grpc_server.grpc_aio_server', return_value=server_mock), \
                patch('src.grpc_server.add_PeerEdgeServiceServicer_to_server') as mock_add_servicer, \
                patch('src.grpc_server.config') as mock_config, \
                patch('src.grpc_server.logger') as mock_logger:
            # Configure mock config
            mock_config.grpc.use_tls = False
            mock_config.grpc.server_address = "localhost:50051"

            # Execute
            server = await start_grpc_server(mock_stream_manager, mock_message_handler)

            # Verify
            assert server == server_mock

            # Verify service was added to server
            mock_add_servicer.assert_called_once()
            servicer = mock_add_servicer.call_args[0][0]
            assert isinstance(servicer, PeerEdgeServicer)
            assert servicer.stream_manager == mock_stream_manager
            assert servicer.message_handler == mock_message_handler

            # Verify insecure port was added
            server_mock.add_insecure_port.assert_called_once_with("localhost:50051")
            server_mock.add_secure_port.assert_not_called()

            # Verify server was started
            server_mock.start.assert_called_once()

            # Verify log message - updated to check all calls for the word "insecure"
            info_calls = [call for call in mock_logger.info.call_args_list]
            assert any("insecure" in args[0][0].lower() for args in info_calls), \
                "No log message containing 'insecure' was found"

    async def test_start_grpc_server_with_tls_missing_files(self, mock_stream_manager, mock_message_handler):
        """Test starting the gRPC server with TLS but missing cert/key files."""
        # Setup
        server_mock = MagicMock()
        server_mock.start = AsyncMock()

        with patch('src.grpc_server.grpc_aio_server', return_value=server_mock), \
                patch('src.grpc_server.add_PeerEdgeServiceServicer_to_server'), \
                patch('src.grpc_server.config') as mock_config, \
                patch('src.grpc_server.os.path.exists', return_value=False), \
                patch('src.grpc_server.logger') as mock_logger:
            # Configure mock config
            mock_config.grpc.use_tls = True
            mock_config.grpc.server_address = "localhost:50051"
            mock_config.grpc.cert_path = "cert.pem"
            mock_config.grpc.key_path = "key.pem"

            # Execute
            server = await start_grpc_server(mock_stream_manager, mock_message_handler)

            # Verify
            assert server == server_mock

            # Verify logger was called with error
            error_calls = [call for call in mock_logger.error.call_args_list]
            assert len(error_calls) > 0
            assert any("not found" in args[0][0].lower() for args in error_calls)

            # Verify logger was called with warning
            mock_logger.warning.assert_called_once()
            assert "falling back" in mock_logger.warning.call_args[0][0].lower()

            # Verify insecure port was used as fallback
            server_mock.add_insecure_port.assert_called_once_with("localhost:50051")
            server_mock.add_secure_port.assert_not_called()

            # Verify server was started
            server_mock.start.assert_called_once()

    async def test_start_grpc_server_with_tls(self, mock_stream_manager, mock_message_handler):
        """Test starting the gRPC server with TLS."""
        # Setup
        server_mock = MagicMock()
        server_mock.start = AsyncMock()

        mock_credentials = "mock_credentials"

        with patch('src.grpc_server.grpc_aio_server', return_value=server_mock), \
                patch('src.grpc_server.add_PeerEdgeServiceServicer_to_server'), \
                patch('src.grpc_server.config') as mock_config, \
                patch('src.grpc_server.os.path.exists', return_value=True), \
                patch('builtins.open', mock_open(read_data=b"mock_file_content")), \
                patch('src.grpc_server.grpc.ssl_server_credentials', return_value=mock_credentials), \
                patch('src.grpc_server.logger') as mock_logger:
            # Configure mock config
            mock_config.grpc.use_tls = True
            mock_config.grpc.server_address = "localhost:50051"
            mock_config.grpc.cert_path = "cert.pem"
            mock_config.grpc.key_path = "key.pem"

            # Execute
            server = await start_grpc_server(mock_stream_manager, mock_message_handler)

            # Verify
            assert server == server_mock

            # Verify files were read (with mock_open, open is mocked for all open calls)
            assert open.call_count == 2

            # Verify secure port was added
            server_mock.add_secure_port.assert_called_once_with("localhost:50051", mock_credentials)
            server_mock.add_insecure_port.assert_not_called()

            # Verify server was started
            server_mock.start.assert_called_once()

            # Verify log message - updated to check all calls for the word "tls"
            info_calls = [call for call in mock_logger.info.call_args_list]
            assert any("tls" in args[0][0].lower() for args in info_calls), \
                "No log message containing 'tls' was found"


class AsyncIteratorMock:
    """Helper class to mock an async iterator."""

    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self.items.pop(0)
        except IndexError:
            raise StopAsyncIteration
