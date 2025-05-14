"""
Circuit breaker implementation for the Giggityflix Edge Service.

This module provides a circuit breaker pattern implementation to prevent
cascading failures when dependent services are experiencing issues.
"""
import asyncio
import functools
import logging
from enum import Enum
from typing import Any, Callable, Dict, List, Type, TypeVar

import pybreaker

logger = logging.getLogger(__name__)


# Circuit breaker states
class CircuitState(str, Enum):
    """Circuit breaker state enum."""
    CLOSED = "closed"  # Normal operation, requests flow through
    OPEN = "open"  # Failing state, requests are short-circuited
    HALF_OPEN = "half_open"  # Testing state, limited requests flow through


# Circuit breaker registry
CIRCUIT_BREAKERS: Dict[str, pybreaker.CircuitBreaker] = {}


def get_circuit_breaker(name: str,
                        failure_threshold: int = 5,
                        recovery_timeout: int = 30,
                        expected_exceptions: List[Type[Exception]] = None) -> pybreaker.CircuitBreaker:
    """
    Get or create a circuit breaker.
    
    Args:
        name: The circuit breaker name
        failure_threshold: Number of failures before opening the circuit
        recovery_timeout: Seconds to wait before trying to recover
        expected_exceptions: Exceptions that are considered failures
        
    Returns:
        CircuitBreaker: The circuit breaker instance
    """
    if name not in CIRCUIT_BREAKERS:
        # Define listeners for logging
        class LogListener(pybreaker.CircuitBreakerListener):
            def state_change(self, cb, old_state, new_state):
                logger.warning(f"Circuit {name} changed from {old_state} to {new_state}")

            def failure(self, cb, exc):
                logger.error(f"Circuit {name} recorded a failure: {exc}")

            def success(self, cb):
                if cb.current_state != pybreaker.STATE_CLOSED:
                    logger.info(f"Circuit {name} recorded a success")

        # Create a new circuit breaker
        if expected_exceptions is None:
            expected_exceptions = [Exception]

        CIRCUIT_BREAKERS[name] = pybreaker.CircuitBreaker(
            fail_max=failure_threshold,
            reset_timeout=recovery_timeout,
            exclude=expected_exceptions,
            listeners=[LogListener()]
        )

    return CIRCUIT_BREAKERS[name]


# TypeVar for function return type
F = TypeVar('F', bound=Callable)


def circuit_protected(name: str,
                      failure_threshold: int = 5,
                      recovery_timeout: int = 30,
                      expected_exceptions: List[Type[Exception]] = None) -> Callable[[F], F]:
    """
    Decorator to protect a function with a circuit breaker.
    
    Args:
        name: The circuit breaker name
        failure_threshold: Number of failures before opening the circuit
        recovery_timeout: Seconds to wait before trying to recover
        expected_exceptions: Exceptions that are considered failures
        
    Returns:
        Decorated function
    """
    circuit = get_circuit_breaker(name, failure_threshold, recovery_timeout, expected_exceptions)

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            @circuit
            async def protected_call():
                return await func(*args, **kwargs)

            return await protected_call()

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            @circuit
            def protected_call():
                return func(*args, **kwargs)

            return protected_call()

        # Return the appropriate wrapper based on whether the function is async or not
        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore

    return decorator


async def with_circuit_breaker(name: str, func: Callable, *args, **kwargs) -> Any:
    """
    Execute a function with circuit breaker protection.
    
    Args:
        name: The circuit breaker name
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Any: The return value of the function
        
    Raises:
        pybreaker.CircuitBreakerError: If the circuit is open
    """
    circuit = get_circuit_breaker(name)

    @circuit
    async def protected_call():
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)

    return await protected_call()
