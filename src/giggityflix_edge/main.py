import asyncio
import logging
import signal

from .server import EdgeServer
from .manager import EdgeManager
from .handlers import MessageHandler

logger = logging.getLogger(__name__)


async def main():
    """Edge service entry point."""
    logging.basicConfig(level=logging.INFO)

    # Initialize components
    handler = MessageHandler()
    manager = EdgeManager(handler)
    server = EdgeServer(manager, "0.0.0.0:50051")

    # Start components
    await manager.start()
    await server.start()

    # Set up signal handlers
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(
            server, manager, stop_event)))

    # Wait for shutdown
    await stop_event.wait()


async def shutdown(server, manager, stop_event):
    """Shuts down components."""
    await server.stop()
    await manager.stop()
    stop_event.set()


if __name__ == "__main__":
    asyncio.run(main())