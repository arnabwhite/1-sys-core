import asyncio
import logging
from typing import List, Dict, Any

logger = logging.getLogger("PubSub")

class PubSubManager:
    def __init__(self):
        self._listeners: List[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        """
        Subscribe a new client queue.
        """
        queue = asyncio.Queue()
        self._listeners.append(queue)
        logger.info(f"New client subscribed to SSE stream. Total listeners: {len(self._listeners)}")
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        """
        Unsubscribe a client queue.
        """
        if queue in self._listeners:
            self._listeners.remove(queue)
            logger.info(f"Client unsubscribed. Total listeners: {len(self._listeners)}")

    async def publish(self, event_type: str, data: Dict[str, Any]):
        """
        Publish an event to all subscribed clients.
        """
        if not self._listeners:
            return
            
        message = {
            "event": event_type,
            "data": data
        }
        
        # We use asyncio.gather to publish to all queues concurrently
        # We capture exceptions to prevent a single failing queue from blocking others
        tasks = [queue.put(message) for queue in self._listeners]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

# Global pubsub instance
pubsub = PubSubManager()
