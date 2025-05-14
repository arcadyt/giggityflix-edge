from unittest.mock import patch, MagicMock, PropertyMock

import pybreaker
import pytest

from src.utils.circuit_breaker import (
    get_circuit_breaker, circuit_protected, with_circuit_breaker, CircuitState
)


@pytest.mark.skip
class TestCircuitBreaker:

    def test_circuit_state_enum(self):
        """Test the CircuitState enum values."""
        assert CircuitState.CLOSED == "closed"
        assert CircuitState.OPEN == "open"
        assert CircuitState.HALF_OPEN == "half_open"

    def test_get_circuit_breaker_new(self):
        """Test getting a new circuit breaker."""
        # Setup - ensure we're starting with a clean registry
        with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
            # Execute
            circuit = get_circuit_breaker("test_circuit")

            # Verify
            assert isinstance(circuit, pybreaker.CircuitBreaker)
            assert circuit.fail_max == 5  # Default value
            assert circuit.reset_timeout == 30  # Default value

            # Verify it was added to the registry
            with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS') as mock_registry:
                mock_registry.__getitem__.return_value = circuit
                assert get_circuit_breaker("test_circuit") == circuit

    def test_get_circuit_breaker_existing(self):
        """Test getting an existing circuit breaker."""
        # Setup - create a circuit first
        with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
            circuit1 = get_circuit_breaker("test_circuit")

            # Execute - get the same circuit again
            circuit2 = get_circuit_breaker("test_circuit")

            # Verify
            assert circuit1 is circuit2  # Same instance

    def test_get_circuit_breaker_custom_params(self):
        """Test getting a circuit breaker with custom parameters."""
        # Setup - ensure we're starting with a clean registry
        with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
            # Execute
            circuit = get_circuit_breaker(
                "test_circuit",
                failure_threshold=10,
                recovery_timeout=60,
                expected_exceptions=[ValueError, KeyError]
            )

            # Verify
            assert isinstance(circuit, pybreaker.CircuitBreaker)
            assert circuit.fail_max == 10
            assert circuit.reset_timeout == 60

            # Check that expected_exceptions was properly configured
            # In pybreaker, 'exclude' is the list of exceptions to NOT count as failures
            # So we need to check that our exceptions are NOT in the exclude list
            assert ValueError not in circuit.exclude
            assert KeyError not in circuit.exclude

            # Check that Exception (the default) is in the exclude list
            assert Exception in circuit.exclude

    def test_get_circuit_breaker_with_listener(self):
        """Test that a circuit breaker gets a listener."""
        # Setup - ensure we're starting with a clean registry
        with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
            # Execute
            circuit = get_circuit_breaker("test_circuit")

            # Verify
            assert len(circuit.listeners) == 1
            assert isinstance(circuit.listeners[0], pybreaker.CircuitBreakerListener)

    @pytest.mark.asyncio
    async def test_circuit_protected_async_success(self):
        """Test circuit_protected decorator with async function that succeeds."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test async function with the decorator
            @circuit_protected("test_circuit")
            async def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Mock the circuit's __call__ method to pass through to the decorated function
            async def mock_call(func):
                return await func()

            mock_circuit.__call__ = mock_call

            # Execute
            result = await test_func("hello", arg2="world")

            # Verify
            assert result == "hello-world"

    def test_circuit_protected_sync_success(self):
        """Test circuit_protected decorator with sync function that succeeds."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test sync function with the decorator
            @circuit_protected("test_circuit")
            def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Mock the circuit's __call__ method to pass through to the decorated function
            def mock_call(func):
                return func()

            mock_circuit.__call__ = mock_call

            # Execute
            result = test_func("hello", arg2="world")

            # Verify
            assert result == "hello-world"

    @pytest.mark.asyncio
    async def test_circuit_protected_async_failure(self):
        """Test circuit_protected decorator with async function that fails."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test async function with the decorator
            @circuit_protected("test_circuit")
            async def test_func():
                raise ValueError("Test error")

            # Mock the circuit's __call__ method to pass through to the decorated function
            async def mock_call(func):
                return await func()

            mock_circuit.__call__ = mock_call

            # Execute and verify exception is propagated
            with pytest.raises(ValueError, match="Test error"):
                await test_func()

    @pytest.mark.asyncio
    async def test_circuit_protected_circuit_open(self):
        """Test circuit_protected decorator when circuit is open."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test async function with the decorator
            @circuit_protected("test_circuit")
            async def test_func():
                return "success"

            # Mock the circuit's __call__ method to raise CircuitBreakerError
            async def mock_call(func):
                raise pybreaker.CircuitBreakerError("Circuit is open")

            mock_circuit.__call__ = mock_call

            # Execute and verify CircuitBreakerError is propagated
            with pytest.raises(pybreaker.CircuitBreakerError, match="Circuit is open"):
                await test_func()

    @pytest.mark.asyncio
    async def test_with_circuit_breaker_async(self):
        """Test with_circuit_breaker helper with async function."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test async function
            async def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Mock the circuit's __call__ method to pass through to the decorated function
            async def mock_call(func):
                return await func()

            mock_circuit.__call__ = mock_call

            # Execute
            result = await with_circuit_breaker("test_circuit", test_func, "hello", arg2="world")

            # Verify
            assert result == "hello-world"

    @pytest.mark.asyncio
    async def test_with_circuit_breaker_sync(self):
        """Test with_circuit_breaker helper with sync function."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test sync function
            def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Mock the circuit's __call__ method to pass through to the decorated function
            async def mock_call(func):
                return func()

            mock_circuit.__call__ = mock_call

            # Execute
            result = await with_circuit_breaker("test_circuit", test_func, "hello", arg2="world")

            # Verify
            assert result == "hello-world"

    @pytest.mark.asyncio
    async def test_with_circuit_breaker_failure(self):
        """Test with_circuit_breaker helper with function that fails."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test function that raises an exception
            def test_func():
                raise ValueError("Test error")

            # Mock the circuit's __call__ method to pass through to the decorated function
            async def mock_call(func):
                return func()

            mock_circuit.__call__ = mock_call

            # Execute and verify exception is propagated
            with pytest.raises(ValueError, match="Test error"):
                await with_circuit_breaker("test_circuit", test_func)

    @pytest.mark.asyncio
    async def test_with_circuit_breaker_circuit_open(self):
        """Test with_circuit_breaker helper when circuit is open."""
        # Setup
        mock_circuit = MagicMock()

        with patch('src.utils.circuit_breaker.get_circuit_breaker', return_value=mock_circuit):
            # Define a test function
            def test_func():
                return "success"

            # Mock the circuit's __call__ method to raise CircuitBreakerError
            async def mock_call(func):
                raise pybreaker.CircuitBreakerError("Circuit is open")

            mock_circuit.__call__ = mock_call

            # Execute and verify CircuitBreakerError is propagated
            with pytest.raises(pybreaker.CircuitBreakerError, match="Circuit is open"):
                await with_circuit_breaker("test_circuit", test_func)


class TestCircuitBreakerListener:
    """Test the built-in CircuitBreakerListener."""

    def test_listener_state_change(self):
        """Test listener state_change method."""
        # Setup
        with patch('src.utils.circuit_breaker.logger') as mock_logger:
            with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
                # Get a circuit breaker with its listener
                circuit = get_circuit_breaker("test_circuit")
                listener = circuit.listeners[0]

                # Call the state_change method
                listener.state_change(circuit, "old_state", "new_state")

                # Verify
                mock_logger.warning.assert_called_once()
                log_msg = mock_logger.warning.call_args[0][0]
                assert "Circuit test_circuit changed from old_state to new_state" == log_msg

    def test_listener_failure(self):
        """Test listener failure method."""
        # Setup
        with patch('src.utils.circuit_breaker.logger') as mock_logger:
            with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
                # Get a circuit breaker with its listener
                circuit = get_circuit_breaker("test_circuit")
                listener = circuit.listeners[0]

                # Call the failure method
                listener.failure(circuit, ValueError("Test error"))

                # Verify
                mock_logger.error.assert_called_once()
                log_msg = mock_logger.error.call_args[0][0]
                assert "Circuit test_circuit recorded a failure: Test error" == log_msg

    def test_listener_success_closed(self):
        """Test listener success method when circuit is closed."""
        # Setup
        with patch('src.utils.circuit_breaker.logger') as mock_logger:
            with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
                # Get a circuit breaker with its listener
                circuit = get_circuit_breaker("test_circuit")
                listener = circuit.listeners[0]

                # Instead of trying to modify the property directly, 
                # mock the property's getter method to return CLOSED state
                with patch.object(type(circuit), 'current_state',
                                  new_callable=PropertyMock,
                                  return_value=pybreaker.STATE_CLOSED):
                    # Call the success method
                    listener.success(circuit)

                    # Verify - should not log when circuit is closed
                    mock_logger.info.assert_not_called()

    def test_listener_success_not_closed(self):
        """Test listener success method when circuit is half-open."""
        # Setup
        with patch('src.utils.circuit_breaker.logger') as mock_logger:
            with patch('src.utils.circuit_breaker.CIRCUIT_BREAKERS', {}):
                # Get a circuit breaker with its listener
                circuit = get_circuit_breaker("test_circuit")
                listener = circuit.listeners[0]

                # Instead of trying to modify the property directly, 
                # mock the property's getter method to return HALF_OPEN state
                with patch.object(type(circuit), 'current_state',
                                  new_callable=PropertyMock,
                                  return_value=pybreaker.STATE_HALF_OPEN):
                    # Call the success method
                    listener.success(circuit)

                    # Verify - should log when circuit is not closed
                    mock_logger.info.assert_called_once()
                    log_msg = mock_logger.info.call_args[0][0]
                    assert "Circuit test_circuit recorded a success" == log_msg
