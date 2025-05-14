import json
# Mock protobuf modules until we generate them
import sys
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.modules['giggityflix_protos'] = MagicMock()
sys.modules['giggityflix_protos.peer_edge_pb2'] = MagicMock()
sys.modules['giggityflix_protos.peer_edge_pb2_grpc'] = MagicMock()

from src.stream_manager import StreamManager
from src.message_handler import MessageHandler
from src.kafka.producer import KafkaProducer
from src.kafka.consumer import KafkaConsumer


# ====== Test Data Fixtures ======

@pytest.fixture
def test_data():
    """Central fixture providing test data constants used across tests."""
    return {
        "peer_id": "test-peer-123",
        "edge_id": "test-edge-456",
        "catalog_uuid": "test-catalog-789",
        "request_id": "test-request-012",
        "timestamp": datetime.now().isoformat(),
    }


# ====== Mock Service Fixtures ======

@pytest.fixture
def mock_kafka_producer():
    """Mock KafkaProducer for testing."""
    producer_mock = MagicMock()
    producer_mock.produce.return_value = None
    producer_mock.flush.return_value = None

    kafka_producer_mock = MagicMock(spec=KafkaProducer)
    kafka_producer_mock.producer = producer_mock
    kafka_producer_mock.publish_peer_lifecycle_event = AsyncMock()
    kafka_producer_mock.publish_peer_catalog_update = AsyncMock()
    kafka_producer_mock.publish_file_delete_response = AsyncMock()
    kafka_producer_mock.publish_file_hash_response = AsyncMock()
    kafka_producer_mock.publish_deadletter = AsyncMock()

    return kafka_producer_mock


@pytest.fixture
def mock_kafka_consumer():
    """Mock KafkaConsumer for testing."""
    consumer_mock = MagicMock(spec=KafkaConsumer)
    consumer_mock.start_consuming.return_value = None
    consumer_mock.stop_consuming.return_value = None
    return consumer_mock


@pytest.fixture
def mock_stream_manager(mock_kafka_producer):
    """Mock StreamManager for testing."""
    stream_manager = MagicMock(spec=StreamManager)
    stream_manager.kafka_producer = mock_kafka_producer
    stream_manager.register_peer.return_value = None
    stream_manager.unregister_peer = AsyncMock()
    stream_manager.send_message_to_peer = AsyncMock(return_value=True)
    stream_manager.is_peer_connected.return_value = True
    stream_manager.get_connected_peers.return_value = ["test-peer-123"]
    return stream_manager


@pytest.fixture
def mock_message_handler(mock_stream_manager, mock_kafka_producer):
    """Mock MessageHandler for testing."""
    handler = MagicMock(spec=MessageHandler)
    handler.stream_manager = mock_stream_manager
    handler.kafka_producer = mock_kafka_producer
    handler.handle_peer_registration = AsyncMock()
    handler.handle_file_delete_response = AsyncMock()
    handler.handle_file_hash_response = AsyncMock()
    handler.handle_batch_file_offer = AsyncMock()
    handler.handle_screenshot_capture_response = AsyncMock()
    handler.handle_edge_command = AsyncMock()
    return handler


# ====== gRPC Test Fixtures ======

@pytest.fixture
def mock_grpc_context():
    """Mock gRPC ServicerContext for testing."""
    context = MagicMock()
    context.abort = AsyncMock()
    context.cancel = AsyncMock()
    context.write = AsyncMock()
    return context


@pytest.fixture
def mock_peer_message():
    """Mock PeerMessage for testing."""
    message = MagicMock()
    message.request_id = "test-request-id"

    # Mock registration request
    reg_request = MagicMock()
    reg_request.peer_name = "test-peer-123"
    reg_request.catalog_uuids = ["catalog-1", "catalog-2"]
    message.registration_request = reg_request

    # Setup HasField method to check for different message types
    def has_field(field_name):
        if field_name == 'registration_request':
            return True
        elif field_name == 'file_delete_response':
            return False
        elif field_name == 'file_hash_response':
            return False
        elif field_name == 'batch_file_offer_request':
            return False
        elif field_name == 'screenshot_capture_response':
            return False
        return False

    message.HasField = has_field

    return message


@pytest.fixture
def mock_edge_message():
    """Mock EdgeMessage for testing."""
    message = MagicMock()
    message.request_id = "test-request-id"
    return message


# ====== Kafka Test Helpers ======

class KafkaMockMessage:
    """Mock class for Kafka messages."""

    def __init__(self, topic, value, error=None):
        self._topic = topic
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


def create_kafka_message(topic, value_dict, error=None):
    """Create a mock Kafka message with the specified topic and value."""

    # Handle datetime serialization
    def serialize(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    value_bytes = json.dumps(value_dict, default=serialize).encode("utf-8")
    return KafkaMockMessage(topic, value_bytes, error)


def create_kafka_error(error_code):
    """Create a mock Kafka error with the specified error code."""
    error = MagicMock()
    error.code.return_value = error_code
    return error
