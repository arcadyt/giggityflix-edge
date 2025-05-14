import asyncio
import logging
import signal
import sys
from typing import List

from giggityflix_edge.config import config
from giggityflix_edge.grpc_server import start_grpc_server
from giggityflix_edge.kafka.consumer import KafkaConsumer
from giggityflix_edge.kafka.producer import KafkaProducer
from giggityflix_edge.message_handler import MessageHandler
from giggityflix_edge.stream_manager import StreamManager

logger = logging.getLogger(__name__)


async def heartbeat_task(stream_manager: StreamManager, kafka_producer: KafkaProducer) -> None:
    """
    Periodically publish heartbeats and catalog updates for connected peers.
    
    Args:
        stream_manager: The stream manager
        kafka_producer: The Kafka producer
    """
    while True:
        try:
            connected_peers = stream_manager.get_connected_peers()
            for peer_id in connected_peers:
                # In a real implementation, we would query the peer for its current catalog
                # For now, we just publish an empty update to show the peer is still connected
                catalog_ids: List[str] = []  # This would be populated with actual data

                await kafka_producer.publish_peer_catalog_update(
                    peer_id=peer_id,
                    catalog_ids=catalog_ids
                )

                logger.debug(f"Published heartbeat for peer {peer_id}")

            # Sleep until next heartbeat
            await asyncio.sleep(config.edge.heartbeat_interval_sec)

        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in heartbeat task: {e}")
            await asyncio.sleep(5)  # Short sleep on error before retrying


async def startup() -> tuple:
    """
    Start all components of the edge service.
    
    Returns:
        tuple: (grpc_server, kafka_consumer, tasks)
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(f"Starting Giggityflix Edge Service (Edge ID: {config.edge.edge_id})")

    # Create Kafka producer
    kafka_producer = KafkaProducer()

    # Create stream manager
    stream_manager = StreamManager(kafka_producer)

    # Create message handler
    message_handler = MessageHandler(stream_manager, kafka_producer)

    # Start gRPC server
    grpc_server = await start_grpc_server(stream_manager, message_handler)

    # Create Kafka consumer
    kafka_consumer = KafkaConsumer(message_handler)
    kafka_consumer.start_consuming()

    # Start heartbeat task
    heartbeat = asyncio.create_task(heartbeat_task(stream_manager, kafka_producer))

    return grpc_server, kafka_consumer, [heartbeat]


async def shutdown(grpc_server, kafka_consumer, tasks: List[asyncio.Task]) -> None:
    """
    Gracefully shut down all components.
    
    Args:
        grpc_server: The gRPC server
        kafka_consumer: The Kafka consumer
        tasks: List of background tasks
    """
    logger.info("Shutting down edge service...")

    # Cancel all background tasks
    for task in tasks:
        if not task.done():
            task.cancel()

    # Wait for tasks to complete with timeout
    if tasks:
        await asyncio.wait(tasks, timeout=5)

    # Shut down gRPC server
    await grpc_server.stop(5)

    # Shut down Kafka consumer
    kafka_consumer.stop_consuming()

    logger.info("Edge service shutdown complete")


async def main() -> None:
    """
    Main entry point for the edge service.
    """
    try:
        # Start all components
        grpc_server, kafka_consumer, tasks = await startup()

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(grpc_server, kafka_consumer, tasks))
            )

        # Keep the service running until cancelled
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logger.info("Service was cancelled")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
