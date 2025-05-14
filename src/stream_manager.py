import asyncio
import logging
import random
from typing import Dict, Optional, Callable, Awaitable, Set

import grpc
from grpc import aio as grpc_aio

from src.config import config

logger = logging.getLogger(__name__)


class StreamManager:
    """
    Manages peer gRPC streams and connection state.
    """
    def __init__(self, kafka_producer):
        self.peer_streams: Dict[str, grpc_aio.StreamStreamCall] = {}
        self.peer_contexts: Dict[str, grpc_aio.ServicerContext] = {}
        self.kafka_producer = kafka_producer
        self._active_tasks: Set[asyncio.Task] = set()
        
    def register_peer(self, peer_id: str, context: grpc_aio.ServicerContext) -> None:
        """
        Register a new peer connection.
        
        Args:
            peer_id: The unique identifier for the peer
            context: The gRPC servicer context for the connection
        """
        logger.info(f"Registering peer {peer_id}")
        self.peer_streams[peer_id] = context
        self.peer_contexts[peer_id] = context
        
        # Publish peer connected event to Kafka
        # Instead of creating a task directly, call our helper method that's more testable
        self._schedule_task(
            self.kafka_producer.publish_peer_lifecycle_event(
                peer_id=peer_id,
                event_type="connected"
            )
        )
    
    def _schedule_task(self, coro):
        """
        Schedule a coroutine to run as a task if possible.
        Falls back to a safer method for testing contexts.
        
        Args:
            coro: The coroutine to schedule
        """
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
            
            # Check if the loop is running
            if loop.is_running():
                # Create a task if the loop is running
                task = asyncio.create_task(coro)
                self._track_task(task)
            else:
                # For testing, just run the coroutine in the background
                # This won't block but also won't complete immediately
                asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:
            # No event loop available (likely in a test)
            # Just log that we would have scheduled a task
            logger.debug(f"No running event loop available, event publishing will be skipped: {coro}")
            # In a real application, we might want to queue this for later or use a different approach
        
    async def unregister_peer(self, peer_id: str) -> None:
        """
        Unregister a peer when the connection is closed.
        
        Args:
            peer_id: The unique identifier for the peer
        """
        logger.info(f"Unregistering peer {peer_id}")
        if peer_id in self.peer_streams:
            del self.peer_streams[peer_id]
            
        if peer_id in self.peer_contexts:
            del self.peer_contexts[peer_id]
            
        # Publish peer disconnected event to Kafka
        await self.kafka_producer.publish_peer_lifecycle_event(
            peer_id=peer_id,
            event_type="disconnected"
        )
    
    async def send_message_to_peer(
        self, 
        peer_id: str, 
        message,
        retry_callback: Optional[Callable[[str, object], Awaitable[None]]] = None
    ) -> bool:
        """
        Send a message to a connected peer.
        
        Args:
            peer_id: The unique identifier for the peer
            message: The message to send
            retry_callback: Optional callback for retrying failed sends
        
        Returns:
            bool: True if the message was sent successfully, False otherwise
        """
        if peer_id not in self.peer_streams:
            logger.warning(f"Cannot send message to peer {peer_id}: not connected")
            return False
        
        try:
            await self.peer_streams[peer_id].write(message)
            return True
        except Exception as e:
            logger.error(f"Failed to send message to peer {peer_id}: {e}")
            
            # If a retry callback is provided, schedule it
            if retry_callback:
                task = asyncio.create_task(retry_callback(peer_id, message))
                self._track_task(task)
                
            return False
    
    def is_peer_connected(self, peer_id: str) -> bool:
        """
        Check if a peer is currently connected.
        
        Args:
            peer_id: The unique identifier for the peer
            
        Returns:
            bool: True if the peer is connected, False otherwise
        """
        return peer_id in self.peer_streams
    
    def get_connected_peers(self) -> list:
        """
        Get a list of all currently connected peer IDs.
        
        Returns:
            list: List of peer IDs
        """
        return list(self.peer_streams.keys())
    
    def _track_task(self, task: asyncio.Task) -> None:
        """
        Track an asyncio task to prevent it from being garbage collected.
        
        Args:
            task: The asyncio task to track
        """
        self._active_tasks.add(task)
        task.add_done_callback(self._remove_task)
    
    def _remove_task(self, task: asyncio.Task) -> None:
        """
        Remove a completed task from tracking.
        
        Args:
            task: The completed asyncio task
        """
        self._active_tasks.discard(task)
