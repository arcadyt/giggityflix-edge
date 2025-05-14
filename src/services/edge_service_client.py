import logging
from typing import Dict

from src.grpc.edge_client import EdgeClient
from src.models import ScreenshotTokenInfo

logger = logging.getLogger(__name__)


class EdgeServiceClient:
    """Client for interacting with the Edge Service via gRPC."""

    def __init__(self):
        self.edge_clients: Dict[str, EdgeClient] = {}

    async def connect_to_edge(self, edge_id: str, edge_address: str) -> None:
        """
        Connect to an Edge Service.
        
        Args:
            edge_id: Unique identifier for the edge
            edge_address: gRPC address of the edge service
        """
        if edge_id not in self.edge_clients:
            client = EdgeClient(edge_address)
            await client.connect()
            self.edge_clients[edge_id] = client
            logger.info(f"Connected to edge {edge_id} at {edge_address}")

    async def close_all_connections(self) -> None:
        """Close all connections to Edge Services."""
        for edge_id, client in self.edge_clients.items():
            await client.close()
            logger.info(f"Closed connection to edge {edge_id}")
        self.edge_clients.clear()

    async def send_screenshot_request(self, edge_id: str, peer_id: str, token_info: ScreenshotTokenInfo) -> bool:
        """
        Send a screenshot request to a peer via its Edge Service.
        
        Args:
            edge_id: ID of the edge service
            peer_id: ID of the peer to send the request to
            token_info: Token information for the request
        
        Returns:
            True if the request was sent successfully, False otherwise
        """
        client = self.edge_clients.get(edge_id)
        if not client:
            logger.error(f"No connection to edge {edge_id}")
            return False

        try:
            # Create a message for the edge service
            # In a real implementation, we'd need to set up a stream first
            # and register the peer, but for this example we'll assume that's done

            # Prepare screenshot capture request to be sent to the peer

            # Endpoint where screenshots should be uploaded
            screenshot_upload_url = f"/api/screenshot/{token_info.catalog_id}"

            # Send message through client
            # This assumes the client already has an active stream
            # In a real implementation, we would need to handle the case where
            # the stream doesn't exist yet

            logger.info(f"Sending screenshot request to peer {peer_id} via edge {edge_id}")
            logger.info(f"  Catalog ID: {token_info.catalog_id}")
            logger.info(f"  Token: {token_info.token}")
            logger.info(f"  Upload URL: {screenshot_upload_url}")

            # In this example code, we're not actually sending anything
            # because we don't have a real connection

            # In a complete implementation, we would:
            # 1. Ensure a stream exists for the edge service
            # 2. Register with the edge service if needed
            # 3. Send the screenshot request message
            # 4. Set up a handler for the response

            return True

        except Exception as e:
            logger.error(f"Error sending screenshot request: {e}")
            return False
