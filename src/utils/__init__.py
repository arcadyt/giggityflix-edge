# Utilities package
"""
Utility functions for the Giggityflix Edge Service.
Includes error handling, metrics, and other helpers.
"""

from src.utils.error_handler import exponential_backoff, safe_execute, log_exceptions
from src.utils.metrics import (
    track_peer_connection, track_peer_disconnection, track_message, 
    track_kafka_message, track_error, time_function, time_kafka_publish
)
from src.utils.circuit_breaker import (
    get_circuit_breaker, circuit_protected, with_circuit_breaker, CircuitState
)

__all__ = [
    # Error handling
    'exponential_backoff',
    'safe_execute',
    'log_exceptions',
    
    # Metrics
    'track_peer_connection',
    'track_peer_disconnection', 
    'track_message',
    'track_kafka_message',
    'track_error',
    'time_function',
    'time_kafka_publish',
    
    # Circuit breaker
    'get_circuit_breaker',
    'circuit_protected',
    'with_circuit_breaker',
    'CircuitState',
]
