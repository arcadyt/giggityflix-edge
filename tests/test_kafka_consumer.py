import json
from unittest.mock import patch, MagicMock, AsyncMock

from confluent_kafka import KafkaError

from giggityflix_edge.config import config
from giggityflix_edge.kafka.consumer import KafkaConsumer


class TestKafkaConsumer:

    def test_init(self):
        """Test KafkaConsumer initialization."""
        # Setup
        message_handler = MagicMock()

        # Execute
        consumer = KafkaConsumer(message_handler)

        # Verify
        assert consumer.message_handler == message_handler
        assert consumer.running is False
        assert consumer.consumer is None
        assert consumer.consumer_thread is None
        assert consumer.loop is None

    def test_start_consuming_already_running(self):
        """Test start_consuming when already running."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)
        consumer.running = True  # Already running

        # Execute
        with patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
            consumer.start_consuming()

            # Verify
            mock_logger.warning.assert_called_once()
            assert "already running" in mock_logger.warning.call_args[0][0]

    def test_start_consuming(self):
        """Test starting the Kafka consumer."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)

        with patch('giggityflix_edge.kafka.consumer.Consumer') as mock_consumer_class, \
                patch('giggityflix_edge.kafka.consumer.threading.Thread') as mock_thread_class, \
                patch('asyncio.get_event_loop') as mock_get_loop:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            mock_thread = MagicMock()
            mock_thread_class.return_value = mock_thread

            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop

            # Execute
            consumer.start_consuming()

            # Verify
            assert consumer.running is True
            assert consumer.loop == mock_loop
            mock_consumer_class.assert_called_once()

            # Check correct topic was subscribed to
            expected_topic = config.kafka.edge_commands_topic_pattern.format(config.edge.edge_id)
            mock_consumer.subscribe.assert_called_once_with([expected_topic])

            # Verify thread creation
            mock_thread_class.assert_called_once()
            thread_kwargs = mock_thread_class.call_args[1]
            assert thread_kwargs['target'] == consumer._consume_loop
            assert thread_kwargs['daemon'] is True
            mock_thread.start.assert_called_once()

    def test_stop_consuming_not_running(self):
        """Test stopping the consumer when not running."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)
        consumer.running = False

        # Execute
        consumer.stop_consuming()

        # Verify nothing happened (early return)
        assert consumer.running is False

    def test_stop_consuming_running(self):
        """Test stopping the consumer when running."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)
        consumer.running = True
        consumer.consumer_thread = MagicMock()

        # Execute
        with patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
            consumer.stop_consuming()

            # Verify
            assert consumer.running is False
            consumer.consumer_thread.join.assert_called_once_with(timeout=5.0)
            mock_logger.info.assert_called_with("Kafka consumer stopped")

    def test_stop_consuming_thread_hanging(self):
        """Test stopping the consumer when thread doesn't terminate gracefully."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)
        consumer.running = True

        # Mock thread that is still alive after join
        mock_thread = MagicMock()
        mock_thread.join = MagicMock()
        mock_thread.is_alive.return_value = True
        consumer.consumer_thread = mock_thread

        # Execute
        with patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
            consumer.stop_consuming()

            # Verify
            assert consumer.running is False
            mock_thread.join.assert_called_once_with(timeout=5.0)
            mock_logger.warning.assert_called_once()
            assert "did not terminate gracefully" in mock_logger.warning.call_args[0][0]

    def test_consume_loop_no_message(self):
        """Test consume loop when poll returns no message."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)

        # Set up consumer to return None, then set running to False
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        consumer.consumer = mock_consumer

        # Set running to True initially, then False after one iteration
        consumer.running = True

        def stop_after_poll(*args, **kwargs):
            consumer.running = False
            return None

        mock_consumer.poll.side_effect = stop_after_poll

        # Execute
        consumer._consume_loop()

        # Verify poll was called
        mock_consumer.poll.assert_called_once_with(timeout=1.0)

        # Verify no processing happened
        assert message_handler.handle_edge_command.call_count == 0

    def test_consume_loop_kafka_error_partition_eof(self):
        """Test consume loop with KafkaError._PARTITION_EOF."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)

        # Create message with PARTITION_EOF error
        mock_message = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_message.error.return_value = mock_error

        # Set up consumer to return message with error, then set running to False
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_message
        consumer.consumer = mock_consumer

        # Set running to True initially, then False after one iteration
        consumer.running = True

        def stop_after_poll(*args, **kwargs):
            consumer.running = False
            return mock_message

        mock_consumer.poll.side_effect = stop_after_poll

        # Execute
        consumer._consume_loop()

        # Verify poll was called
        mock_consumer.poll.assert_called_once_with(timeout=1.0)

        # Verify no processing happened
        assert message_handler.handle_edge_command.call_count == 0

    def test_consume_loop_kafka_error_other(self):
        """Test consume loop with other KafkaError."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)

        # Create message with other error
        mock_message = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._ALL_BROKERS_DOWN
        mock_message.error.return_value = mock_error

        # Set up consumer to return message with error, then set running to False
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_message
        consumer.consumer = mock_consumer

        # Set running to True initially, then False after one iteration
        consumer.running = True

        def stop_after_poll(*args, **kwargs):
            consumer.running = False
            return mock_message

        mock_consumer.poll.side_effect = stop_after_poll

        # Execute
        with patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
            consumer._consume_loop()

            # Verify poll was called
            mock_consumer.poll.assert_called_once_with(timeout=1.0)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            assert "Kafka consumer error" in mock_logger.error.call_args[0][0]

            # Verify no processing happened
            assert message_handler.handle_edge_command.call_count == 0

    def test_consume_loop_valid_message(self):
        """Test consume loop processing a valid message."""
        # Setup
        message_handler = MagicMock()
        message_handler.handle_edge_command = AsyncMock()
        consumer = KafkaConsumer(message_handler)

        # Create valid message
        message_content = {"key": "value"}
        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(message_content).encode('utf-8')

        # Set up consumer to return valid message, then set running to False
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_message
        consumer.consumer = mock_consumer

        # Set running to True initially, then False after one iteration
        consumer.running = True

        def stop_after_poll(*args, **kwargs):
            consumer.running = False
            return mock_message

        mock_consumer.poll.side_effect = stop_after_poll

        # Mock asyncio.run_coroutine_threadsafe
        mock_future = MagicMock()
        mock_future.result.return_value = None

        # Create a mock event loop
        mock_loop = MagicMock()
        consumer.loop = mock_loop

        # Execute
        with patch('asyncio.run_coroutine_threadsafe', return_value=mock_future) as mock_run_threadsafe:
            consumer._consume_loop()

            # Verify poll was called
            mock_consumer.poll.assert_called_once_with(timeout=1.0)

            # Verify message was processed
            mock_run_threadsafe.assert_called_once()
            # Check that the first argument is a coroutine from handle_edge_command
            assert message_handler.handle_edge_command.call_count == 1
            assert message_handler.handle_edge_command.call_args[0][0] == message_content

            # Check that the second argument is the event loop
            assert mock_run_threadsafe.call_args[0][1] == mock_loop

            # Verify future.result was called with timeout
            mock_future.result.assert_called_once_with(timeout=10)

    def test_consume_loop_message_processing_error(self):
        """Test consume loop with error during message processing."""
        # Setup
        message_handler = MagicMock()
        message_handler.handle_edge_command = AsyncMock()
        consumer = KafkaConsumer(message_handler)

        # Create valid message
        message_content = {"key": "value"}
        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(message_content).encode('utf-8')

        # Set up consumer to return valid message, then set running to False
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_message
        consumer.consumer = mock_consumer

        # Set running to True initially, then False after one iteration
        consumer.running = True

        def stop_after_poll(*args, **kwargs):
            consumer.running = False
            return mock_message

        mock_consumer.poll.side_effect = stop_after_poll

        # Mock asyncio.run_coroutine_threadsafe to raise an exception
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Test processing error")

        # Create a mock event loop
        mock_loop = MagicMock()
        consumer.loop = mock_loop

        # Execute
        with patch('asyncio.run_coroutine_threadsafe', return_value=mock_future) as mock_run_threadsafe, \
                patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
            consumer._consume_loop()

            # Verify poll was called
            mock_consumer.poll.assert_called_once_with(timeout=1.0)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            assert "Failed to process message" in mock_logger.error.call_args[0][0]

    def test_consume_loop_kafka_exception(self):
        """Test consume loop with KafkaException."""
        # Setup
        message_handler = MagicMock()
        consumer = KafkaConsumer(message_handler)

        # Set up consumer to raise KafkaException
        mock_consumer = MagicMock()

        # Create a patch for the confluent_kafka.KafkaException class
        with patch('giggityflix_edge.kafka.consumer.KafkaException', Exception):
            # Make poll raise our mocked KafkaException
            mock_consumer.poll.side_effect = Exception("Kafka error")
            consumer.consumer = mock_consumer
            consumer.running = True

            # Create a flag to stop after one iteration
            counter = [0]

            def side_effect(*args, **kwargs):
                counter[0] += 1
                if counter[0] > 1:
                    consumer.running = False
                raise Exception("Kafka error")

            mock_consumer.poll.side_effect = side_effect

            # Execute
            with patch('giggityflix_edge.kafka.consumer.logger') as mock_logger:
                consumer._consume_loop()

                # Verify error was logged
                mock_logger.error.assert_called()
                error_logs = [call[0][0] for call in mock_logger.error.call_args_list]
                assert any("Kafka exception" in msg for msg in error_logs)

                # Verify consumer was closed
                mock_consumer.close.assert_called_once()
