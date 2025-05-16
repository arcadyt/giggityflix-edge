"""
Metrics utilities for the Giggityflix Edge Service.

This module provides helper functions and classes for capturing 
metrics using Prometheus for observability and monitoring.
"""
import functools
import time
from typing import Callable, TypeVar, cast

from prometheus_client import Counter, Gauge, Histogram

# Define metrics
PEER_CONNECTIONS = Gauge(
    'edge_peer_connections',
    'Number of connected peers',
    ['edge_id']
)

PEER_CONNECTION_DURATION = Histogram(
    'edge_peer_connection_duration_seconds',
    'Duration of peer connections in seconds',
    ['edge_id', 'peer_id'],
    buckets=(5, 30, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400)
)

MESSAGE_COUNTER = Counter(
    'edge_messages_total',
    'Number of messages processed',
    ['edge_id', 'direction', 'message_type']
)

MESSAGE_SIZE = Histogram(
    'edge_message_size_bytes',
    'Size of messages in bytes',
    ['edge_id', 'direction', 'message_type'],
    buckets=(10, 100, 1000, 10000, 100000, 1000000, 10000000)
)

MESSAGE_PROCESSING_TIME = Histogram(
    'edge_message_processing_time_seconds',
    'Time to process messages in seconds',
    ['edge_id', 'message_type'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10)
)

KAFKA_MESSAGE_COUNTER = Counter(
    'edge_kafka_messages_total',
    'Number of Kafka messages processed',
    ['edge_id', 'direction', 'topic']
)

KAFKA_PUBLISH_TIME = Histogram(
    'edge_kafka_publish_time_seconds',
    'Time to publish Kafka messages in seconds',
    ['edge_id', 'topic'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10)
)

ERROR_COUNTER = Counter(
    'edge_errors_total',
    'Number of errors encountered',
    ['edge_id', 'error_type']
)

# TypeVar for function return type
F = TypeVar('F', bound=Callable)


def track_peer_connection(edge_id: str) -> None:
    """
    Track a new peer connection.
    
    Args:
        edge_id: The edge ID
    """
    PEER_CONNECTIONS.labels(edge_id=edge_id).inc()


def track_peer_disconnection(edge_id: str, peer_id: str, duration_seconds: float) -> None:
    """
    Track a peer disconnection.
    
    Args:
        edge_id: The edge ID
        peer_id: The peer ID
        duration_seconds: The connection duration in seconds
    """
    PEER_CONNECTIONS.labels(edge_id=edge_id).dec()
    PEER_CONNECTION_DURATION.labels(edge_id=edge_id, peer_id=peer_id).observe(duration_seconds)


def track_message(edge_id: str, direction: str, message_type: str, size_bytes: int) -> None:
    """
    Track a message.
    
    Args:
        edge_id: The edge ID
        direction: The message direction ('inbound' or 'outbound')
        message_type: The message type
        size_bytes: The message size in bytes
    """
    MESSAGE_COUNTER.labels(edge_id=edge_id, direction=direction, message_type=message_type).inc()
    MESSAGE_SIZE.labels(edge_id=edge_id, direction=direction, message_type=message_type).observe(size_bytes)


def track_kafka_message(edge_id: str, direction: str, topic: str) -> None:
    """
    Track a Kafka message.
    
    Args:
        edge_id: The edge ID
        direction: The message direction ('published' or 'consumed')
        topic: The Kafka topic
    """
    KAFKA_MESSAGE_COUNTER.labels(edge_id=edge_id, direction=direction, topic=topic).inc()


def track_error(edge_id: str, error_type: str) -> None:
    """
    Track an error.
    
    Args:
        edge_id: The edge ID
        error_type: The error type
    """
    ERROR_COUNTER.labels(edge_id=edge_id, error_type=error_type).inc()


def time_function(edge_id: str, message_type: str) -> Callable[[F], F]:
    """
    Decorator to time a function and record processing time.
    
    Args:
        edge_id: The edge ID
        message_type: The message type
        
    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            MESSAGE_PROCESSING_TIME.labels(
                edge_id=edge_id,
                message_type=message_type
            ).observe(duration)
            return result

        return cast(F, wrapper)

    return decorator


def time_kafka_publish(edge_id: str, topic: str) -> Callable[[F], F]:
    """
    Decorator to time Kafka publishing and record time.
    
    Args:
        edge_id: The edge ID
        topic: The Kafka topic
        
    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            KAFKA_PUBLISH_TIME.labels(
                edge_id=edge_id,
                topic=topic
            ).observe(duration)
            return result

        return cast(F, wrapper)

    return decorator
