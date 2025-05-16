import logging
from typing import Optional

from giggityflix_grpc_peer import EdgeMessage, file_operations, media

logger = logging.getLogger(__name__)


class KafkaAdapter:
    """Converts between Kafka and gRPC messages."""

    def convert(self, kafka_message: dict) -> Optional[EdgeMessage]:
        """Converts Kafka message to gRPC message."""
        try:
            message_type = kafka_message.get('type')
            request_id = kafka_message.get('request_id', '')

            message = EdgeMessage(
                request_id=request_id,
                edge_id=kafka_message.get('edge_id', '')
            )

            if message_type == 'file_delete':
                message.file_delete_request.CopyFrom(
                    self._create_file_delete_request(kafka_message)
                )
            elif message_type == 'file_hash':
                message.file_hash_request.CopyFrom(
                    self._create_file_hash_request(kafka_message)
                )
            elif message_type == 'file_remap':
                message.file_remap_request.CopyFrom(
                    self._create_file_remap_request(kafka_message)
                )
            elif message_type == 'screenshot':
                message.screenshot_capture_request.CopyFrom(
                    self._create_screenshot_request(kafka_message)
                )
            else:
                logger.error(f"Unknown message type: {message_type}")
                return None

            return message
        except Exception as e:
            logger.error(f"Conversion error: {e}")
            return None

    def _create_file_delete_request(self, kafka_message: dict) -> file_operations.FileDeleteRequest:
        """Creates file delete request."""
        return file_operations.FileDeleteRequest(
            catalog_ids=kafka_message.get('catalog_ids', [])
        )

    def _create_file_hash_request(self, kafka_message: dict) -> file_operations.FileHashRequest:
        """Creates file hash request."""
        return file_operations.FileHashRequest(
            catalog_id=kafka_message.get('catalog_id', ''),
            hash_types=kafka_message.get('hash_types', [])
        )

    def _create_file_remap_request(self, kafka_message: dict) -> file_operations.FileRemapRequest:
        """Creates file remap request."""
        return file_operations.FileRemapRequest(
            old_catalog_id=kafka_message.get('old_catalog_id', ''),
            new_catalog_id=kafka_message.get('new_catalog_id', '')
        )

    def _create_screenshot_request(self, kafka_message: dict) -> media.ScreenshotCaptureRequest:
        """Creates screenshot request."""
        return media.ScreenshotCaptureRequest(
            catalog_id=kafka_message.get('catalog_id', ''),
            quantity=kafka_message.get('quantity', 1),
            upload_token=kafka_message.get('upload_token', ''),
            upload_endpoint=kafka_message.get('upload_endpoint', '')
        )