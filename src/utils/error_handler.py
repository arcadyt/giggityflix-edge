"""
Error handling utilities for the Giggityflix Edge Service.

This module provides helper functions and classes for error handling,
including retry logic, circuit breaking, and error logging.
"""
import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, Optional, Type, TypeVar, cast

from src.config import config

logger = logging.getLogger(__name__)

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


def exponential_backoff(max_retries: int = None,
                        initial_delay_ms: int = None,
                        max_delay_ms: int = None,
                        jitter_factor: float = None,
                        exceptions: Type[Exception] = Exception) -> Callable[[F], F]:
    """
    Decorator for retrying a function with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        initial_delay_ms: Initial delay in milliseconds
        max_delay_ms: Maximum delay in milliseconds
        jitter_factor: Factor to apply random jitter to delay
        exceptions: Exception types to catch and retry
        
    Returns:
        Decorated function
    """
    # Use config values if not provided
    if max_retries is None:
        max_retries = config.retry.max_retries
    if initial_delay_ms is None:
        initial_delay_ms = config.retry.initial_delay_ms
    if max_delay_ms is None:
        max_delay_ms = config.retry.max_delay_ms
    if jitter_factor is None:
        jitter_factor = config.retry.jitter_factor

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for retry in range(max_retries + 1):  # +1 for the initial attempt
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if retry >= max_retries:
                        break

                    # Calculate delay with exponential backoff and jitter
                    base_delay_ms = min(
                        initial_delay_ms * (2 ** retry),
                        max_delay_ms
                    )
                    jitter_ms = base_delay_ms * jitter_factor * random.random()
                    delay_ms = base_delay_ms + jitter_ms

                    logger.warning(
                        f"Retry {retry + 1}/{max_retries} for {func.__name__} in {delay_ms:.2f}ms: {e}"
                    )

                    # Wait before retrying
                    await asyncio.sleep(delay_ms / 1000)

            # If we get here, all retries failed
            logger.error(f"All retries failed for {func.__name__}: {last_exception}")
            raise last_exception

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            for retry in range(max_retries + 1):  # +1 for the initial attempt
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if retry >= max_retries:
                        break

                    # Calculate delay with exponential backoff and jitter
                    base_delay_ms = min(
                        initial_delay_ms * (2 ** retry),
                        max_delay_ms
                    )
                    jitter_ms = base_delay_ms * jitter_factor * random.random()
                    delay_ms = base_delay_ms + jitter_ms

                    logger.warning(
                        f"Retry {retry + 1}/{max_retries} for {func.__name__} in {delay_ms:.2f}ms: {e}"
                    )

                    # Wait before retrying
                    time.sleep(delay_ms / 1000)

            # If we get here, all retries failed
            logger.error(f"All retries failed for {func.__name__}: {last_exception}")
            raise last_exception

        # Return the appropriate wrapper based on whether the function is async or not
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        else:
            return cast(F, sync_wrapper)

    return decorator


async def safe_execute(func: Callable, *args, **kwargs) -> Optional[Any]:
    """
    Safely execute a function and log any exceptions.
    
    Args:
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The return value of the function or None if an exception occurs
    """
    try:
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error executing {func.__name__}: {e}")
        return None


def log_exceptions(func: F) -> F:
    """
    Decorator to log exceptions from a function.
    
    Args:
        func: The function to decorate
        
    Returns:
        Decorated function
    """

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Exception in {func.__name__}: {e}")
            raise

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Exception in {func.__name__}: {e}")
            raise

    # Return the appropriate wrapper based on whether the function is async or not
    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, sync_wrapper)
