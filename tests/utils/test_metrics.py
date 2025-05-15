import time
from unittest.mock import patch, MagicMock

from giggityflix_edge.utils.metrics import (
    track_peer_connection, track_peer_disconnection, track_message,
    track_kafka_message, track_error, time_function, time_kafka_publish
)


class TestMetrics:

    def test_track_peer_connection(self):
        """Test tracking a new peer connection."""
        with patch('giggityflix_edge.utils.metrics.PEER_CONNECTIONS') as mock_gauge:
            # Setup mock gauge
            mock_labels = MagicMock()
            mock_gauge.labels.return_value = mock_labels

            # Execute
            track_peer_connection("edge-1")

            # Verify
            mock_gauge.labels.assert_called_once_with(edge_id="edge-1")
            mock_labels.inc.assert_called_once()

    def test_track_peer_disconnection(self):
        """Test tracking a peer disconnection."""
        with patch('giggityflix_edge.utils.metrics.PEER_CONNECTIONS') as mock_connections_gauge, \
                patch('giggityflix_edge.utils.metrics.PEER_CONNECTION_DURATION') as mock_duration_hist:
            # Setup mock gauges
            mock_connections_labels = MagicMock()
            mock_connections_gauge.labels.return_value = mock_connections_labels

            mock_duration_labels = MagicMock()
            mock_duration_hist.labels.return_value = mock_duration_labels

            # Execute
            track_peer_disconnection("edge-1", "peer-1", 120.5)

            # Verify
            mock_connections_gauge.labels.assert_called_once_with(edge_id="edge-1")
            mock_connections_labels.dec.assert_called_once()

            mock_duration_hist.labels.assert_called_once_with(edge_id="edge-1", peer_id="peer-1")
            mock_duration_labels.observe.assert_called_once_with(120.5)

    def test_track_message(self):
        """Test tracking a message."""
        with patch('giggityflix_edge.utils.metrics.MESSAGE_COUNTER') as mock_counter, \
                patch('giggityflix_edge.utils.metrics.MESSAGE_SIZE') as mock_hist:
            # Setup mocks
            mock_counter_labels = MagicMock()
            mock_counter.labels.return_value = mock_counter_labels

            mock_hist_labels = MagicMock()
            mock_hist.labels.return_value = mock_hist_labels

            # Execute
            track_message("edge-1", "inbound", "registration", 1024)

            # Verify
            mock_counter.labels.assert_called_once_with(
                edge_id="edge-1", direction="inbound", message_type="registration"
            )
            mock_counter_labels.inc.assert_called_once()

            mock_hist.labels.assert_called_once_with(
                edge_id="edge-1", direction="inbound", message_type="registration"
            )
            mock_hist_labels.observe.assert_called_once_with(1024)

    def test_track_kafka_message(self):
        """Test tracking a Kafka message."""
        with patch('giggityflix_edge.utils.metrics.KAFKA_MESSAGE_COUNTER') as mock_counter:
            # Setup mock
            mock_counter_labels = MagicMock()
            mock_counter.labels.return_value = mock_counter_labels

            # Execute
            track_kafka_message("edge-1", "published", "peer-lifecycle-events")

            # Verify
            mock_counter.labels.assert_called_once_with(
                edge_id="edge-1", direction="published", topic="peer-lifecycle-events"
            )
            mock_counter_labels.inc.assert_called_once()

    def test_track_error(self):
        """Test tracking an error."""
        with patch('giggityflix_edge.utils.metrics.ERROR_COUNTER') as mock_counter:
            # Setup mock
            mock_counter_labels = MagicMock()
            mock_counter.labels.return_value = mock_counter_labels

            # Execute
            track_error("edge-1", "network")

            # Verify
            mock_counter.labels.assert_called_once_with(
                edge_id="edge-1", error_type="network"
            )
            mock_counter_labels.inc.assert_called_once()

    def test_time_function_decorator(self):
        """Test time_function decorator."""
        with patch('giggityflix_edge.utils.metrics.MESSAGE_PROCESSING_TIME') as mock_hist:
            # Setup mock
            mock_hist_labels = MagicMock()
            mock_hist.labels.return_value = mock_hist_labels

            # Define a decorated function
            @time_function("edge-1", "test_message")
            def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Execute
            result = test_func("hello", arg2="world")

            # Verify
            assert result == "hello-world"
            mock_hist.labels.assert_called_once_with(
                edge_id="edge-1", message_type="test_message"
            )
            mock_hist_labels.observe.assert_called_once()

            # Verify the observed duration is a reasonable time value
            duration = mock_hist_labels.observe.call_args[0][0]
            assert duration >= 0

    def test_time_function_actual_timing(self):
        """Test time_function decorator actually measures time."""
        with patch('giggityflix_edge.utils.metrics.MESSAGE_PROCESSING_TIME') as mock_hist:
            # Setup mock
            mock_hist_labels = MagicMock()
            mock_hist.labels.return_value = mock_hist_labels

            # Define a decorated function that sleeps
            @time_function("edge-1", "test_message")
            def slow_func():
                time.sleep(0.05)  # Sleep for 50ms
                return "done"

            # Execute
            result = slow_func()

            # Verify
            assert result == "done"

            # Verify the observed duration is at least the sleep time
            duration = mock_hist_labels.observe.call_args[0][0]
            assert duration >= 0.05

    def test_time_kafka_publish_decorator(self):
        """Test time_kafka_publish decorator."""
        with patch('giggityflix_edge.utils.metrics.KAFKA_PUBLISH_TIME') as mock_hist:
            # Setup mock
            mock_hist_labels = MagicMock()
            mock_hist.labels.return_value = mock_hist_labels

            # Define a decorated function
            @time_kafka_publish("edge-1", "test-topic")
            def test_func(arg1, arg2=None):
                return f"{arg1}-{arg2}"

            # Execute
            result = test_func("hello", arg2="world")

            # Verify
            assert result == "hello-world"
            mock_hist.labels.assert_called_once_with(
                edge_id="edge-1", topic="test-topic"
            )
            mock_hist_labels.observe.assert_called_once()

            # Verify the observed duration is a reasonable time value
            duration = mock_hist_labels.observe.call_args[0][0]
            assert duration >= 0

    def test_time_kafka_publish_actual_timing(self):
        """Test time_kafka_publish decorator actually measures time."""
        with patch('giggityflix_edge.utils.metrics.KAFKA_PUBLISH_TIME') as mock_hist:
            # Setup mock
            mock_hist_labels = MagicMock()
            mock_hist.labels.return_value = mock_hist_labels

            # Define a decorated function that sleeps
            @time_kafka_publish("edge-1", "test-topic")
            def slow_func():
                time.sleep(0.05)  # Sleep for 50ms
                return "done"

            # Execute
            result = slow_func()

            # Verify
            assert result == "done"

            # Verify the observed duration is at least the sleep time
            duration = mock_hist_labels.observe.call_args[0][0]
            assert duration >= 0.05
