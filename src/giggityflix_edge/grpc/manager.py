import asyncio
import logging
from typing import Dict, Optional

from giggityflix_grpc_peer import EdgeMessage, PeerMessage, EdgeWebRTCMessage, PeerWebRTCMessage
from .handlers import MessageHandler
from .adapters import KafkaAdapter

logger = logging.getLogger(__name__)


class EdgeManager:
    """Manages peer connections and message routing."""

    def __init__(self, message_handler: MessageHandler, max_peers: int = 1000):
        self.peers = {}
        self.handler = message_handler
        self.max_peers = max_peers
        self.kafka_adapter = KafkaAdapter()
        self._heartbeat_task = None

    async def start(self):
        """Starts manager operations."""
        self._heartbeat_task = asyncio.create_task(self._check_peer_heartbeats())

    async def stop(self):
        """Stops manager operations."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        # Disconnect all peers
        for peer_id in list(self.peers.keys()):
            await self.disconnect_peer(peer_id)

    async def validate_peer(self, peer_id: str) -> bool:
        """Validates peer eligibility for connection."""
        if len(self.peers) >= self.max_peers:
            logger.warning(f"Max connections reached: {peer_id}")
            return False

        if peer_id in self.peers:
            logger.warning(f"Peer already connected: {peer_id}")
            return False

        # Check with tracker if connected elsewhere
        if await self._check_connected_elsewhere(peer_id):
            logger.warning(f"Peer connected elsewhere: {peer_id}")
            return False

        return True

    async def _check_connected_elsewhere(self, peer_id: str) -> bool:
        """Checks if peer is connected to another edge."""
        # Placeholder - would query tracker service
        return False

    async def register_peer(self, peer_id: str, context) -> bool:
        """Registers new peer connection."""
        self.peers[peer_id] = {
            'context': context,
            'last_activity': asyncio.get_event_loop().time()
        }

        # Publish connection event
        await self._publish_connection_event(peer_id)
        logger.info(f"Peer registered: {peer_id}")
        return True

    async def disconnect_peer(self, peer_id: str) -> None:
        """Disconnects peer."""
        if peer_id in self.peers:
            del self.peers[peer_id]
            await self._publish_disconnection_event(peer_id)
            logger.info(f"Peer disconnected: {peer_id}")

    def is_peer_connected(self, peer_id: str) -> bool:
        """Checks if peer is connected."""
        return peer_id in self.peers

    async def _publish_connection_event(self, peer_id: str) -> None:
        """Publishes peer connection event to Kafka."""
        # Placeholder - would publish to Kafka
        pass

    async def _publish_disconnection_event(self, peer_id: str) -> None:
        """Publishes peer disconnection event to Kafka."""
        # Placeholder - would publish to Kafka
        pass

    async def process_peer_message(self, peer_id: str, message: PeerMessage) -> None:
        """Processes message from peer."""
        if peer_id in self.peers:
            self.peers[peer_id]['last_activity'] = asyncio.get_event_loop().time()
            await self.handler.handle_peer_message(peer_id, message)

    async def process_webrtc_message(self, peer_id: str, message: EdgeWebRTCMessage) -> Optional[PeerWebRTCMessage]:
        """Processes WebRTC message from peer."""
        if peer_id in self.peers:
            self.peers[peer_id]['last_activity'] = asyncio.get_event_loop().time()
            return await self.handler.handle_webrtc_message(peer_id, message)
        return None

    async def send_message(self, peer_id: str, message: EdgeMessage) -> bool:
        """Sends message to peer."""
        if peer_id not in self.peers:
            logger.warning(f"Cannot send to disconnected peer: {peer_id}")
            return False

        try:
            # In actual implementation, would use response_stream.write
            return True
        except Exception as e:
            logger.error(f"Send error to {peer_id}: {e}")
            return False

    async def handle_kafka_message(self, kafka_message: dict) -> bool:
        """Processes Kafka message."""
        peer_id = kafka_message.get('peer_id')
        if not peer_id or peer_id not in self.peers:
            logger.warning(f"Kafka message for unavailable peer: {peer_id}")
            return False

        # Convert to gRPC message
        message = self.kafka_adapter.convert(kafka_message)
        if not message:
            return False

        # Send to peer
        return await self.send_message(peer_id, message)

    async def _check_peer_heartbeats(self) -> None:
        """Checks for inactive peers."""
        timeout = 120  # seconds
        check_interval = 30  # seconds

        while True:
            try:
                current_time = asyncio.get_event_loop().time()

                for peer_id, data in list(self.peers.items()):
                    if current_time - data['last_activity'] > timeout:
                        logger.warning(f"Peer timeout: {peer_id}")
                        await self.disconnect_peer(peer_id)

                await asyncio.sleep(check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(check_interval)