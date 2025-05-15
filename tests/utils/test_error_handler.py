import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from giggityflix_edge.utils.error_handler import exponential_backoff, safe_execute, log_exceptions


@pytest.mark.asyncio
class TestErrorHandler:

    async def test_exponential_backoff_async_success(self):
        """Test exponential backoff decorator with async function that succeeds."""
        # Setup
        mock_func = AsyncMock(return_value="success")

        # Apply decorator
        decorated_func = exponential_backoff(max_retries=3)(mock_func)

        # Execute
        result = await decorated_func("arg1", key="value")

        # Verify
        assert result == "success"
        mock_func.assert_called_once_with("arg1", key="value")

    async def test_exponential_backoff_async_retry_then_success(self):
        """Test exponential backoff decorator with async function that fails then succeeds."""
        # Setup
        side_effects = [Exception("Fail"), Exception("Fail"), "success"]
        mock_func = AsyncMock(side_effect=side_effects)

        # Apply decorator
        decorated_func = exponential_backoff(
            max_retries=3,
            initial_delay_ms=1,  # Use small values for testing
            max_delay_ms=5,
            jitter_factor=0.0  # No jitter for deterministic testing
        )(mock_func)

        # Execute
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            result = await decorated_func()

            # Verify logger was called for each retry
            assert mock_logger.warning.call_count == 2
            for i in range(2):
                assert f"Retry {i + 1}/3" in mock_logger.warning.call_args_list[i][0][0]

        # Verify
        assert result == "success"
        assert mock_func.call_count == 3

    async def test_exponential_backoff_async_all_retries_fail(self):
        """Test exponential backoff decorator with async function that always fails."""
        # Setup
        mock_func = AsyncMock(side_effect=Exception("Always fails"))

        # Apply decorator
        decorated_func = exponential_backoff(
            max_retries=2,
            initial_delay_ms=1,
            max_delay_ms=5,
            jitter_factor=0.0
        )(mock_func)

        # Execute and verify exception is raised
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            with pytest.raises(Exception, match="Always fails"):
                await decorated_func()

            # Verify logger was called for each retry
            assert mock_logger.warning.call_count == 2
            assert mock_logger.error.call_count == 1
            assert "All retries failed" in mock_logger.error.call_args[0][0]

        # Verify all retries were attempted
        assert mock_func.call_count == 3  # Initial attempt + 2 retries

    def test_exponential_backoff_sync_success(self):
        """Test exponential backoff decorator with sync function that succeeds."""
        # Setup
        mock_func = MagicMock(return_value="success")

        # Apply decorator
        decorated_func = exponential_backoff(max_retries=3)(mock_func)

        # Execute
        result = decorated_func("arg1", key="value")

        # Verify
        assert result == "success"
        mock_func.assert_called_once_with("arg1", key="value")

    def test_exponential_backoff_sync_retry_then_success(self):
        """Test exponential backoff decorator with sync function that fails then succeeds."""
        # Setup
        side_effects = [Exception("Fail"), Exception("Fail"), "success"]
        mock_func = MagicMock(side_effect=side_effects)
        # Add a __name__ attribute to the mock function
        mock_func.__name__ = "mock_func"

        # Apply decorator
        decorated_func = exponential_backoff(
            max_retries=3,
            initial_delay_ms=1,
            max_delay_ms=5,
            jitter_factor=0.0
        )(mock_func)

        # Execute with time.sleep patched to avoid actual delays
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger, \
                patch('giggityflix_edge.utils.error_handler.time.sleep') as mock_sleep:
            result = decorated_func()

            # Verify logger was called for each retry
            assert mock_logger.warning.call_count == 2
            for i in range(2):
                assert f"Retry {i + 1}/3" in mock_logger.warning.call_args_list[i][0][0]

            # Verify sleep was called with increasing delays
            assert mock_sleep.call_count == 2
            delays = [call[0][0] for call in mock_sleep.call_args_list]
            assert delays[1] > delays[0], "Second delay should be longer than first"

        # Verify result and total call count
        assert result == "success"
        assert mock_func.call_count == 3

    def test_exponential_backoff_sync_all_retries_fail(self):
        """Test exponential backoff decorator with sync function that always fails."""
        # Setup
        mock_func = MagicMock(side_effect=Exception("Always fails"))
        # Add a __name__ attribute to the mock function
        mock_func.__name__ = "mock_func"

        # Apply decorator
        decorated_func = exponential_backoff(
            max_retries=2,
            initial_delay_ms=1,
            max_delay_ms=5,
            jitter_factor=0.0
        )(mock_func)

        # Execute and verify exception is raised
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger, \
                patch('giggityflix_edge.utils.error_handler.time.sleep') as mock_sleep:
            with pytest.raises(Exception, match="Always fails"):
                decorated_func()

            # Verify logger was called for each retry
            assert mock_logger.warning.call_count == 2
            assert mock_logger.error.call_count == 1
            assert "All retries failed" in mock_logger.error.call_args[0][0]

            # Verify sleep was called with appropriate delays
            assert mock_sleep.call_count == 2

        # Verify all retries were attempted
        assert mock_func.call_count == 3  # Initial attempt + 2 retries

    def test_exponential_backoff_with_specific_exceptions(self):
        """Test exponential backoff decorator with specific exception types."""
        # PART 1: Test with matching exception type
        # Setup
        side_effects = [ValueError("Bad value"), ValueError("Bad value again"), "success"]
        mock_func = MagicMock(side_effect=side_effects)
        mock_func.__name__ = "mock_func"  # Add __name__ attribute

        # Apply decorator with specific exception type
        decorated_func = exponential_backoff(
            max_retries=3,
            initial_delay_ms=1,
            max_delay_ms=5,
            jitter_factor=0.0,
            exceptions=ValueError  # Only retry for ValueError
        )(mock_func)

        # Execute with time.sleep patched
        with patch('giggityflix_edge.utils.error_handler.time.sleep'):
            result = decorated_func()

        # Verify
        assert result == "success"
        assert mock_func.call_count == 3

        # PART 2: Test with non-matching exception type
        # Setup
        mock_func2 = MagicMock(side_effect=TypeError("Wrong type"))
        mock_func2.__name__ = "mock_func2"  # Add __name__ attribute

        decorated_func2 = exponential_backoff(
            max_retries=3,
            initial_delay_ms=1,
            max_delay_ms=5,
            jitter_factor=0.0,
            exceptions=ValueError  # Only retry for ValueError, not TypeError
        )(mock_func2)

        # Execute and verify exception is raised (no retry)
        with pytest.raises(TypeError, match="Wrong type"):
            decorated_func2()

        # Verify only called once (no retries)
        assert mock_func2.call_count == 1

    async def test_safe_execute_async_success(self):
        """Test safe_execute with async function that succeeds."""

        # Setup
        async def test_func(arg1, arg2=None):
            return f"{arg1}-{arg2}"

        # Execute
        result = await safe_execute(test_func, "hello", arg2="world")

        # Verify
        assert result == "hello-world"

    async def test_safe_execute_async_failure(self):
        """Test safe_execute with async function that fails."""

        # Setup
        async def test_func():
            raise ValueError("Test error")

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            result = await safe_execute(test_func)

        # Verify
        assert result is None
        mock_logger.error.assert_called_once()
        assert "test_func" in mock_logger.error.call_args[0][0]
        assert "Test error" in mock_logger.error.call_args[0][0]

    def test_safe_execute_sync_success(self):
        """Test safe_execute with sync function that succeeds."""

        # Setup
        def test_func(arg1, arg2=None):
            return f"{arg1}-{arg2}"

        # Execute
        result = asyncio.run(safe_execute(test_func, "hello", arg2="world"))

        # Verify
        assert result == "hello-world"

    def test_safe_execute_sync_failure(self):
        """Test safe_execute with sync function that fails."""

        # Setup
        def test_func():
            raise ValueError("Test error")

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            result = asyncio.run(safe_execute(test_func))

        # Verify
        assert result is None
        mock_logger.error.assert_called_once()
        assert "test_func" in mock_logger.error.call_args[0][0]
        assert "Test error" in mock_logger.error.call_args[0][0]

    async def test_log_exceptions_async(self):
        """Test log_exceptions decorator with async function."""

        # Setup
        async def test_func():
            raise ValueError("Test exception")

        decorated_func = log_exceptions(test_func)

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            with pytest.raises(ValueError, match="Test exception"):
                await decorated_func()

        # Verify exception was logged
        mock_logger.exception.assert_called_once()
        assert "test_func" in mock_logger.exception.call_args[0][0]
        assert "Test exception" in mock_logger.exception.call_args[0][0]

    def test_log_exceptions_sync(self):
        """Test log_exceptions decorator with sync function."""

        # Setup
        def test_func():
            raise ValueError("Test exception")

        decorated_func = log_exceptions(test_func)

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            with pytest.raises(ValueError, match="Test exception"):
                decorated_func()

        # Verify exception was logged
        mock_logger.exception.assert_called_once()
        assert "test_func" in mock_logger.exception.call_args[0][0]
        assert "Test exception" in mock_logger.exception.call_args[0][0]

    async def test_log_exceptions_async_no_error(self):
        """Test log_exceptions decorator with async function that doesn't raise."""

        # Setup
        async def test_func():
            return "success"

        decorated_func = log_exceptions(test_func)

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            result = await decorated_func()

        # Verify
        assert result == "success"
        mock_logger.exception.assert_not_called()

    def test_log_exceptions_sync_no_error(self):
        """Test log_exceptions decorator with sync function that doesn't raise."""

        # Setup
        def test_func():
            return "success"

        decorated_func = log_exceptions(test_func)

        # Execute with logger mocked
        with patch('giggityflix_edge.utils.error_handler.logger') as mock_logger:
            result = decorated_func()

        # Verify
        assert result == "success"
        mock_logger.exception.assert_not_called()
