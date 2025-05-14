import asyncio
import json
import logging
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException

from giggityflix_edge.config import config

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Consumes Kafka messages containing commands for the edge service.
    """

    def __init__(self, message_handler):
        self.message_handler = message_handler
        self.running = False
        self.consumer = None
        self.consumer_thread = None
        self.loop = None

    def start_consuming(self) -> None:
        """
        Start consuming Kafka messages from the edge-specific command topic.
        """
        if self.running:
            logger.warning("Kafka consumer already running")
            return

        self.running = True
        self.loop = asyncio.get_event_loop()

        # Create consumer
        self.consumer = Consumer({
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'group.id': config.kafka.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
        })

        # Subscribe to edge-specific commands topic
        edge_commands_topic = config.kafka.edge_commands_topic_pattern.format(config.edge.edge_id)
        self.consumer.subscribe([edge_commands_topic])

        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_loop,
            daemon=True
        )
        self.consumer_thread.start()

        logger.info(f"Kafka consumer started, listening to topic: {edge_commands_topic}")

    def _consume_loop(self) -> None:
        """
        Main consumer loop. Runs in a separate thread.
        """
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        continue
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                        continue

                # Process message
                try:
                    # Parse message
                    message_json = msg.value().decode('utf-8')
                    message = json.loads(message_json)

                    # Handle message in the main event loop
                    future = asyncio.run_coroutine_threadsafe(
                        self.message_handler.handle_edge_command(message),
                        self.loop
                    )

                    # Wait for the coroutine to complete (with timeout)
                    future.result(timeout=10)

                except Exception as e:
                    logger.error(f"Failed to process message: {e}")

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
        finally:
            if self.consumer and self.running:
                self.consumer.close()
                logger.info("Kafka consumer closed")

    def stop_consuming(self) -> None:
        """
        Stop consuming Kafka messages.
        """
        if not self.running:
            return

        logger.info("Stopping Kafka consumer...")
        self.running = False

        if self.consumer_thread:
            self.consumer_thread.join(timeout=5.0)
            if self.consumer_thread.is_alive():
                logger.warning("Kafka consumer thread did not terminate gracefully")

        logger.info("Kafka consumer stopped")
