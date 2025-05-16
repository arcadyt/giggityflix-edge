import asyncio
import logging
import grpc
from typing import Dict

from giggityflix_grpc_peer import (
    PeerEdgeServiceServicer, add_PeerEdgeServiceServicer_to_server
)
from .manager import EdgeManager

logger = logging.getLogger(__name__)


class PeerEdgeServicer(PeerEdgeServiceServicer):
    """Implements PeerEdgeService gRPC interface."""

    def __init__(self, edge_manager: EdgeManager):
        self.edge_manager = edge_manager

    async def AsyncOperations(self, request_iterator, context):
        """Handles bidirectional streaming RPC."""
        metadata = dict(context.invocation_metadata())
        peer_id = metadata.get('peer_id')

        if not peer_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing peer_id")
            return

        # Validate peer before processing any messages
        if not await self.edge_manager.validate_peer(peer_id):
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Peer validation failed")
            return

        # Register peer
        if not await self.edge_manager.register_peer(peer_id, context):
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Registration failed")
            return

        try:
            # Process stream messages
            async for message in request_iterator:
                await self.edge_manager.process_peer_message(peer_id, message)

        except Exception as e:
            logger.error(f"Stream error: {peer_id}: {e}")
        finally:
            await self.edge_manager.disconnect_peer(peer_id)

    async def WebRTCOperations(self, request, context):
        """Handles WebRTC operations."""
        metadata = dict(context.invocation_metadata())
        peer_id = metadata.get('peer_id')

        if not peer_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing peer_id")
            return None

        if not self.edge_manager.is_peer_connected(peer_id):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Peer not connected")
            return None

        return await self.edge_manager.process_webrtc_message(peer_id, request)


class EdgeServer:
    """gRPC server implementation."""

    def __init__(self, edge_manager: EdgeManager, address: str):
        self.edge_manager = edge_manager
        self.address = address
        self.server = None

    async def start(self):
        """Starts gRPC server."""
        self.server = grpc.aio.server()
        servicer = PeerEdgeServicer(self.edge_manager)
        add_PeerEdgeServiceServicer_to_server(servicer, self.server)
        self.server.add_insecure_port(self.address)
        await self.server.start()
        logger.info(f"Server started: {self.address}")

    async def stop(self):
        """Stops gRPC server."""
        if self.server:
            await self.server.stop(5)
            self.server = None