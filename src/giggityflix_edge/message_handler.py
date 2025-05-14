import asyncio
import logging
import random
import uuid
from typing import Dict, Any, List

from giggityflix_edge.config import config
import giggityflix_grpc_peer.generated.peer_edge.peer_edge_pb2 as pb2

logger = logging.getLogger(__name__)


class MessageHandler:
    """
    Handles message processing for all message types in the edge service.
    """

    def __init__(self, stream_manager, kafka_producer):
        self.stream_manager = stream_manager
        self.kafka_producer = kafka_producer

    async def handle_peer_registration(self, peer_id: str, catalog_uuids: List[str], request_id: str) -> Any:
        """
        Handle a peer registration request.
        
        Args:
            peer_id: The unique identifier for the peer
            catalog_uuids: List of catalog UUIDs the peer has available
            request_id: The request ID for correlation
            
        Returns:
            EdgeMessage: The registration response message
        """
        # Publish peer catalog update to Kafka
        await self.kafka_producer.publish_peer_catalog_update(
            peer_id=peer_id,
            catalog_ids=catalog_uuids
        )

        # Create registration response
        registration_response = pb2.PeerRegistrationResponse(
            peer_name=peer_id,
            edge_name=config.edge.edge_id,
            success=True
        )

        # Create edge message
        edge_message = pb2.EdgeMessage(
            request_id=request_id,
            registration_response=registration_response
        )

        logger.info(f"Registered peer {peer_id} with {len(catalog_uuids)} catalog IDs")
        return edge_message

    async def handle_file_delete_response(self, peer_id: str, file_delete_response) -> None:
        """
        Handle a file delete response from a peer.
        
        Args:
            peer_id: The unique identifier for the peer
            file_delete_response: The file delete response message
        """
        # Publish file delete response to Kafka
        await self.kafka_producer.publish_file_delete_response(
            peer_id=peer_id,
            catalog_uuid=file_delete_response.catalog_uuid,
            success=file_delete_response.success,
            error_message=file_delete_response.error_message
        )

        logger.info(
            f"File delete response from peer {peer_id} for catalog {file_delete_response.catalog_uuid}: "
            f"{'success' if file_delete_response.success else 'failure'}"
        )

    async def handle_file_hash_response(self, peer_id: str, file_hash_response) -> None:
        """
        Handle a file hash response from a peer.
        
        Args:
            peer_id: The unique identifier for the peer
            file_hash_response: The file hash response message
        """
        # Publish file hash response to Kafka
        await self.kafka_producer.publish_file_hash_response(
            peer_id=peer_id,
            catalog_uuid=file_hash_response.catalog_uuid,
            hashes=dict(file_hash_response.hashes),
            error_message=file_hash_response.error_message
        )

        logger.info(
            f"File hash response from peer {peer_id} for catalog {file_hash_response.catalog_uuid} "
            f"with {len(file_hash_response.hashes)} hash types"
        )

    async def handle_batch_file_offer(self, peer_id: str, batch_file_offer_request) -> None:
        """
        Handle a batch file offer request from a peer.
        
        Args:
            peer_id: The unique identifier for the peer
            batch_file_offer_request: The batch file offer request message
        """
        # In a real implementation, the service might process these files 
        # and assign catalog UUIDs, but for now we'll just respond with 
        # placeholder results

        # Extract catalog UUIDs if any were assigned
        # For a real implementation, we'd get these from a catalog service
        file_offer_results = []
        for file_item in batch_file_offer_request.files:
            # Generate a "fake" catalog UUID for demo purposes
            catalog_uuid = str(uuid.uuid4())
            file_offer_results.append(
                pb2.FileOfferResult(
                    peer_luid=file_item.peer_luid,
                    catalog_uuid=catalog_uuid
                )
            )

        # Update peer's catalog
        assigned_catalog_ids = [result.catalog_uuid for result in file_offer_results]
        await self.kafka_producer.publish_peer_catalog_update(
            peer_id=peer_id,
            catalog_ids=assigned_catalog_ids
        )

        logger.info(
            f"Batch file offer from peer {peer_id} with {len(batch_file_offer_request.files)} files "
            f"of type {batch_file_offer_request.category_type}"
        )

    async def handle_screenshot_capture_response(self, peer_id: str, screenshot_capture_response) -> None:
        """
        Handle a screenshot capture response from a peer.
        Note: This is mostly a legacy handler as screenshots are now uploaded directly.
        
        Args:
            peer_id: The unique identifier for the peer
            screenshot_capture_response: The screenshot capture response message
        """
        # In the updated flow, peers upload screenshots directly to the screenshot service
        # This handler is mainly for error reporting

        if screenshot_capture_response.HasField('error_message'):
            logger.error(
                f"Error capturing screenshot from peer {peer_id} for catalog "
                f"{screenshot_capture_response.catalog_uuid}: {screenshot_capture_response.error_message}"
            )
        else:
            logger.info(
                f"Screenshot capture acknowledged from peer {peer_id} for catalog "
                f"{screenshot_capture_response.catalog_uuid}"
            )

    async def handle_edge_command(self, command: Dict[str, Any]) -> None:
        """
        Handle a command received via Kafka.
        
        Args:
            command: The command data
        """
        # Extract command details
        peer_id = command.get('peer_id')
        command_type = command.get('command_type')
        command_data = command.get('command_data', {})
        request_id = command.get('request_id', str(uuid.uuid4()))

        # Check if peer is connected
        if not self.stream_manager.is_peer_connected(peer_id):
            logger.warning(f"Peer {peer_id} not connected, cannot process {command_type} command")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command,
                reason="Peer not connected"
            )
            return

        # Handle command based on type
        try:
            if command_type == 'file_delete':
                await self._handle_file_delete_command(peer_id, command_data, request_id)
            elif command_type == 'file_hash':
                await self._handle_file_hash_command(peer_id, command_data, request_id)
            elif command_type == 'file_remap':
                await self._handle_file_remap_command(peer_id, command_data, request_id)
            elif command_type == 'screenshot_capture':
                await self._handle_screenshot_capture_command(peer_id, command_data, request_id)
            else:
                logger.warning(f"Unknown command type: {command_type}")
                await self.kafka_producer.publish_deadletter(
                    peer_id=peer_id,
                    original_message=command,
                    reason=f"Unknown command type: {command_type}"
                )
        except Exception as e:
            logger.error(f"Error handling {command_type} command for peer {peer_id}: {e}")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command,
                reason=f"Error: {str(e)}"
            )

    async def _handle_file_delete_command(self, peer_id: str, command_data: Dict[str, Any], request_id: str) -> None:
        """
        Handle a file delete command.
        
        Args:
            peer_id: The unique identifier for the peer
            command_data: The command data
            request_id: The request ID for correlation
        """
        catalog_uuids = command_data.get('catalog_uuids', [])

        # Create file delete request
        file_delete_request = pb2.FileDeleteRequest(catalog_uuids=catalog_uuids)

        # Create edge message
        edge_message = pb2.EdgeMessage(
            request_id=request_id,
            file_delete_request=file_delete_request
        )

        # Send message to peer
        success = await self.stream_manager.send_message_to_peer(
            peer_id=peer_id,
            message=edge_message,
            retry_callback=self._create_retry_callback(max_retries=config.retry.max_retries)
        )

        if success:
            logger.info(f"Sent file delete request to peer {peer_id} for {len(catalog_uuids)} catalog IDs")
        else:
            logger.error(f"Failed to send file delete request to peer {peer_id}")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command_data,
                reason="Failed to send message to peer"
            )

    async def _handle_file_hash_command(self, peer_id: str, command_data: Dict[str, Any], request_id: str) -> None:
        """
        Handle a file hash command.
        
        Args:
            peer_id: The unique identifier for the peer
            command_data: The command data
            request_id: The request ID for correlation
        """
        catalog_uuid = command_data.get('catalog_uuid', '')
        hash_types = command_data.get('hash_types', [])

        # Create file hash request
        file_hash_request = pb2.FileHashRequest(
            catalog_uuid=catalog_uuid,
            hash_types=hash_types
        )

        # Create edge message
        edge_message = pb2.EdgeMessage(
            request_id=request_id,
            file_hash_request=file_hash_request
        )

        # Send message to peer
        success = await self.stream_manager.send_message_to_peer(
            peer_id=peer_id,
            message=edge_message,
            retry_callback=self._create_retry_callback(max_retries=config.retry.max_retries)
        )

        if success:
            logger.info(f"Sent file hash request to peer {peer_id} for catalog {catalog_uuid}")
        else:
            logger.error(f"Failed to send file hash request to peer {peer_id}")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command_data,
                reason="Failed to send message to peer"
            )

    async def _handle_file_remap_command(self, peer_id: str, command_data: Dict[str, Any], request_id: str) -> None:
        """
        Handle a file remap command.
        
        Args:
            peer_id: The unique identifier for the peer
            command_data: The command data
            request_id: The request ID for correlation
        """
        old_catalog_uuid = command_data.get('old_catalog_uuid', '')
        new_catalog_uuid = command_data.get('new_catalog_uuid', '')

        # Create file remap request
        file_remap_request = pb2.FileRemapRequest(
            old_catalog_uuid=old_catalog_uuid,
            new_catalog_uuid=new_catalog_uuid
        )

        # Create edge message
        edge_message = pb2.EdgeMessage(
            request_id=request_id,
            file_remap_request=file_remap_request
        )

        # Send message to peer
        success = await self.stream_manager.send_message_to_peer(
            peer_id=peer_id,
            message=edge_message,
            retry_callback=self._create_retry_callback(max_retries=config.retry.max_retries)
        )

        if success:
            logger.info(f"Sent file remap request to peer {peer_id}: {old_catalog_uuid} -> {new_catalog_uuid}")
        else:
            logger.error(f"Failed to send file remap request to peer {peer_id}")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command_data,
                reason="Failed to send message to peer"
            )

    async def _handle_screenshot_capture_command(self, peer_id: str, command_data: Dict[str, Any],
                                                 request_id: str) -> None:
        """
        Handle a screenshot capture command.
        
        Args:
            peer_id: The unique identifier for the peer
            command_data: The command data
            request_id: The request ID for correlation
        """
        catalog_uuid = command_data.get('catalog_uuid', '')
        quantity = command_data.get('quantity', 1)
        upload_token = command_data.get('upload_token', '')
        upload_endpoint = command_data.get('upload_endpoint', '')

        # Create screenshot capture request with all fields from the updated proto
        screenshot_capture_request = pb2.ScreenshotCaptureRequest(
            catalog_uuid=catalog_uuid,
            quantity=quantity,
            upload_token=upload_token,
            upload_endpoint=upload_endpoint,
            request_id=request_id  # Include the request_id as specified in the updated proto
        )

        # Create edge message
        edge_message = pb2.EdgeMessage(
            request_id=request_id,
            screenshot_capture_request=screenshot_capture_request
        )

        # Send message to peer
        success = await self.stream_manager.send_message_to_peer(
            peer_id=peer_id,
            message=edge_message,
            retry_callback=self._create_retry_callback(max_retries=config.retry.max_retries)
        )

        if success:
            logger.info(f"Sent screenshot capture request to peer {peer_id} for catalog {catalog_uuid}")
        else:
            logger.error(f"Failed to send screenshot capture request to peer {peer_id}")
            await self.kafka_producer.publish_deadletter(
                peer_id=peer_id,
                original_message=command_data,
                reason="Failed to send message to peer"
            )

    def _create_retry_callback(self, max_retries: int):
        """
        Create a retry callback for failed message sends.
        
        Args:
            max_retries: Maximum number of retries
            
        Returns:
            Callable: The retry callback function
        """

        async def retry_callback(peer_id: str, message) -> None:
            """
            Retry sending a message to a peer with exponential backoff.
            
            Args:
                peer_id: The unique identifier for the peer
                message: The message to send
            """
            for retry in range(max_retries):
                # Calculate delay with exponential backoff and jitter
                base_delay_ms = min(
                    config.retry.initial_delay_ms * (2 ** retry),
                    config.retry.max_delay_ms
                )
                jitter_ms = base_delay_ms * config.retry.jitter_factor * random.random()
                delay_ms = base_delay_ms + jitter_ms

                # Wait before retrying
                await asyncio.sleep(delay_ms / 1000)

                # Check if peer is still connected
                if not self.stream_manager.is_peer_connected(peer_id):
                    logger.warning(f"Peer {peer_id} not connected, abandoning retry")
                    return

                # Try to send message again
                try:
                    success = await self.stream_manager.send_message_to_peer(
                        peer_id=peer_id,
                        message=message,
                        retry_callback=None  # No more retries to avoid infinite recursion
                    )

                    if success:
                        logger.info(f"Successfully sent message to peer {peer_id} on retry {retry + 1}")
                        return

                except Exception as e:
                    logger.error(f"Error retrying send to peer {peer_id}: {e}")

            # If we get here, all retries failed
            logger.error(f"Failed to send message to peer {peer_id} after {max_retries} retries")
            # Note: We can't access command_data here to publish to deadletter queue,
            # but the message will have been sent to the deadletter queue on the first failure

        return retry_callback
