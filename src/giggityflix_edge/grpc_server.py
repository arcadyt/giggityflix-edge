import asyncio
import logging
import os

import grpc
from grpc.aio import server as grpc_aio_server

from giggityflix_edge.config import config
from giggityflix_grpc_peer.generated.peer_edge.peer_edge_pb2_grpc import PeerEdgeServiceServicer, add_PeerEdgeServiceServicer_to_server
from giggityflix_edge.message_handler import MessageHandler
from giggityflix_edge.stream_manager import StreamManager

logger = logging.getLogger(__name__)


class PeerEdgeServicer(PeerEdgeServiceServicer):
    """
    Implementation of the PeerEdgeService gRPC service.
    Handles bidirectional streaming between peers and the edge service.
    """

    def __init__(self, stream_manager: StreamManager, message_handler: MessageHandler):
        self.stream_manager = stream_manager
        self.message_handler = message_handler

    async def message(self, request_iterator, context):
        """
        Bidirectional streaming RPC for peer-edge communication.
        
        Args:
            request_iterator: Iterator yielding PeerMessage messages
            context: gRPC servicer context
            
        Yields:
            EdgeMessage: Messages to send to the peer
        """
        peer_id = None

        try:
            # Process each incoming message from the peer
            async for peer_message in request_iterator:
                # Extract request ID for correlation
                request_id = peer_message.request_id

                # First message should be a registration request
                if peer_id is None:
                    if not peer_message.HasField('registration_request'):
                        logger.error("First message must be a registration request")
                        await context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                            "First message must be a registration request")
                        return

                    # Handle peer registration
                    reg_request = peer_message.registration_request
                    peer_id = reg_request.peer_name
                    catalog_uuids = reg_request.catalog_uuids

                    # Register peer in the stream manager
                    self.stream_manager.register_peer(peer_id, context)

                    # Handle registration and get response
                    response = await self.message_handler.handle_peer_registration(
                        peer_id, catalog_uuids, request_id
                    )

                    # Send registration response
                    yield response

                    logger.info(f"Peer {peer_id} registered successfully")
                else:
                    # Handle other message types
                    if peer_message.HasField('file_delete_response'):
                        await self.message_handler.handle_file_delete_response(
                            peer_id, peer_message.file_delete_response
                        )

                    elif peer_message.HasField('file_hash_response'):
                        await self.message_handler.handle_file_hash_response(
                            peer_id, peer_message.file_hash_response
                        )

                    elif peer_message.HasField('batch_file_offer_request'):
                        await self.message_handler.handle_batch_file_offer(
                            peer_id, peer_message.batch_file_offer_request
                        )

                    elif peer_message.HasField('screenshot_capture_response'):
                        await self.message_handler.handle_screenshot_capture_response(
                            peer_id, peer_message.screenshot_capture_response
                        )

                    else:
                        logger.warning(f"Received unknown message type from peer {peer_id}")

        except asyncio.CancelledError:
            logger.info(f"Stream with peer {peer_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in bidirectional stream with peer {peer_id}: {e}")
        finally:
            # Unregister peer if it was registered
            if peer_id:
                await self.stream_manager.unregister_peer(peer_id)
                logger.info(f"Peer {peer_id} unregistered")


async def start_grpc_server(stream_manager: StreamManager, message_handler: MessageHandler) -> grpc.aio.Server:
    """
    Start the gRPC server.
    
    Args:
        stream_manager: The stream manager
        message_handler: The message handler
        
    Returns:
        grpc.aio.Server: The started gRPC server
    """
    # Create server
    server = grpc_aio_server(
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),  # 50 MB
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),  # 50 MB
        ]
    )

    # Add service to server
    add_PeerEdgeServiceServicer_to_server(
        PeerEdgeServicer(stream_manager, message_handler), server
    )

    # Set up TLS if configured
    if config.grpc.use_tls:
        # Check if cert and key files exist
        if not os.path.exists(config.grpc.cert_path) or not os.path.exists(config.grpc.key_path):
            logger.error(
                f"TLS certificate and/or key file not found at {config.grpc.cert_path} and {config.grpc.key_path}")
            logger.warning("Falling back to insecure connection")
            server.add_insecure_port(config.grpc.server_address)
        else:
            # Read certificate and key files
            with open(config.grpc.cert_path, 'rb') as f:
                cert_data = f.read()
            with open(config.grpc.key_path, 'rb') as f:
                key_data = f.read()

            # Create server credentials
            server_credentials = grpc.ssl_server_credentials([(key_data, cert_data)])
            server.add_secure_port(config.grpc.server_address, server_credentials)
            logger.info(f"Server configured with TLS at {config.grpc.server_address}")
    else:
        # Add insecure port
        server.add_insecure_port(config.grpc.server_address)
        logger.info(f"Server configured with insecure connection at {config.grpc.server_address}")

    # Start server
    await server.start()
    logger.info(f"gRPC server started on {config.grpc.server_address}")

    return server
