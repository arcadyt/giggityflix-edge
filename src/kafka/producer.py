import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from confluent_kafka import Producer

from src.config import config

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Produces Kafka messages for various edge service events.
    """

    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': f'edge-service-{config.edge.edge_id}'
        })

    async def publish_peer_lifecycle_event(self, peer_id: str, event_type: str) -> None:
        """
        Publish a peer lifecycle event (connected/disconnected).
        
        Args:
            peer_id: The unique identifier for the peer
            event_type: The type of event ('connected' or 'disconnected')
        """
        message = {
            'peer_id': peer_id,
            'edge_id': config.edge.edge_id,
            'event_type': event_type,
            'timestamp': datetime.now().isoformat()
        }

        self._publish_message(config.kafka.peer_lifecycle_events_topic, message)
        logger.info(f"Published peer {event_type} event for {peer_id}")

    async def publish_peer_catalog_update(self, peer_id: str, catalog_ids: List[str]) -> None:
        """
        Publish an update about the media files available on a peer.
        
        Args:
            peer_id: The unique identifier for the peer
            catalog_ids: List of catalog IDs available on the peer
        """
        message = {
            'peer_id': peer_id,
            'edge_id': config.edge.edge_id,
            'catalog_ids': catalog_ids,
            'is_full_update': True,  # Specify if this is a full catalog or incremental update
            'timestamp': datetime.now().isoformat()
        }

        self._publish_message(config.kafka.peer_catalog_updates_topic, message)
        logger.info(f"Published catalog update for peer {peer_id} with {len(catalog_ids)} catalog IDs")

    async def publish_file_delete_response(
            self,
            peer_id: str,
            catalog_uuid: str,
            success: bool,
            error_message: Optional[str] = None
    ) -> None:
        """
        Publish a response for a file deletion operation.
        
        Args:
            peer_id: The unique identifier for the peer
            catalog_uuid: The ID of the catalog that was requested to be deleted
            success: Whether the deletion was successful
            error_message: Optional error message if deletion failed
        """
        message = {
            'peer_id': peer_id,
            'edge_id': config.edge.edge_id,
            'catalog_uuid': catalog_uuid,
            'success': success,
            'error_message': error_message or '',
            'timestamp': datetime.now().isoformat()
        }

        self._publish_message(config.kafka.file_delete_responses_topic, message)
        logger.info(f"Published file delete response for peer {peer_id}, catalog {catalog_uuid}, success: {success}")

    async def publish_file_hash_response(
            self,
            peer_id: str,
            catalog_uuid: str,
            hashes: Dict[str, str],
            error_message: Optional[str] = None
    ) -> None:
        """
        Publish a response containing file hashes.
        
        Args:
            peer_id: The unique identifier for the peer
            catalog_uuid: The ID of the catalog that was hashed
            hashes: Dictionary mapping hash types to hash values
            error_message: Optional error message if hashing failed
        """
        message = {
            'peer_id': peer_id,
            'edge_id': config.edge.edge_id,
            'catalog_uuid': catalog_uuid,
            'hashes': hashes,
            'error_message': error_message or '',
            'timestamp': datetime.now().isoformat()
        }

        self._publish_message(config.kafka.file_hash_responses_topic, message)
        logger.info(f"Published file hash response for peer {peer_id}, catalog {catalog_uuid}")

    async def publish_deadletter(self, peer_id: str, original_message: Dict[str, Any], reason: str) -> None:
        """
        Publish a message to the deadletter queue when delivery fails.
        
        Args:
            peer_id: The unique identifier for the peer
            original_message: The original message that failed to be delivered
            reason: The reason for the delivery failure
        """
        message = {
            'peer_id': peer_id,
            'edge_id': config.edge.edge_id,
            'original_message': original_message,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }

        deadletter_topic = config.kafka.deadletter_topic_pattern.format(config.edge.edge_id)
        self._publish_message(deadletter_topic, message)
        logger.warning(f"Published message to deadletter queue for peer {peer_id}: {reason}")

    def _publish_message(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Internal method to publish a message to a Kafka topic.
        
        Args:
            topic: The Kafka topic
            message: The message to publish
        """
        try:
            # Convert message to JSON
            message_json = json.dumps(message)

            # Publish message
            self.producer.produce(
                topic,
                value=message_json.encode('utf-8'),
                callback=self._delivery_report
            )

            # Flush to ensure message is sent
            self.producer.flush()

        except Exception as e:
            logger.error(f"Failed to publish message to {topic}: {e}")

    def _delivery_report(self, err, msg) -> None:
        """
        Callback for message delivery reports.
        
        Args:
            err: Error, if any
            msg: The message that was delivered
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
