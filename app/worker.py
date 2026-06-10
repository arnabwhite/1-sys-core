import asyncio
import logging
import traceback
from typing import Set, Optional, Any
from app.database import AsyncSessionLocal
from app.queue_manager import dequeue_task, complete_task, fail_task
from app.tasks_handlers import execute_task
from app.pubsub import pubsub
from app.models import TaskStatus

logger = logging.getLogger("QueueWorker")

class QueueWorker:
    def __init__(self, concurrency: int = 3, poll_interval: float = 1.0):
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.is_running = False
        self._semaphore = asyncio.Semaphore(concurrency)
        self._active_tasks: Set[asyncio.Task] = set()
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self):
        """
        Starts the background polling loop.
        """
        if self.is_running:
            return
        self.is_running = True
        logger.info(f"Starting Queue Worker (concurrency={self.concurrency}, poll_interval={self.poll_interval}s)")
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """
        Stops the worker and waits for active tasks to finish.
        """
        self.is_running = False
        logger.info("Stopping Queue Worker loop...")
        
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
                
        if self._active_tasks:
            logger.info(f"Waiting for {len(self._active_tasks)} active tasks to finish...")
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            logger.info("All active tasks stopped.")

    async def _run_loop(self):
        while self.is_running:
            try:
                # If we are already running at max capacity, wait a bit
                if self._semaphore.locked():
                    await asyncio.sleep(0.1)
                    continue

                async with AsyncSessionLocal() as db:
                    # Try to dequeue a task atomically
                    task = await dequeue_task(db)
                    
                    if task:
                        # Acquire a semaphore slot
                        await self._semaphore.acquire()
                        
                        # Publish start event
                        await pubsub.publish("task_started", task.to_dict())
                        
                        # Spawn task execution asynchronously
                        run_task = asyncio.create_task(
                            self._execute_and_handle(task.id, task.task_type, task.payload)
                        )
                        self._active_tasks.add(run_task)
                        
                        # Release semaphore and remove task from set on completion
                        run_task.add_done_callback(
                            lambda t: (self._semaphore.release(), self._active_tasks.discard(t))
                        )
                        
                        # Instantly check for next task without sleeping (high throughput optimization)
                        continue
                        
            except Exception as e:
                logger.error(f"Error in queue worker polling loop: {e}", exc_info=True)
                
            # Sleep if no task was found or if an error occurred
            await asyncio.sleep(self.poll_interval)

    async def _execute_and_handle(self, task_id: Any, task_type: str, payload: dict):
        logger.info(f"[Worker] Task {task_id} of type '{task_type}' started.")
        
        # Open separate database session for executing and updating this task
        async with AsyncSessionLocal() as db:
            try:
                result = await execute_task(task_type, payload)
                updated_task = await complete_task(db, task_id, result=result)
                logger.info(f"[Worker] Task {task_id} completed successfully. Result: {result}")
                await pubsub.publish("task_completed", updated_task.to_dict())
            except Exception as e:
                # Capture the full traceback
                err_msg = f"{e}\n{traceback.format_exc()}"
                logger.error(f"[Worker] Task {task_id} failed: {e}")
                updated_task = await fail_task(db, task_id, err_msg)
                
                event_name = "task_failed" if updated_task.status == TaskStatus.FAILED else "task_retrying"
                await pubsub.publish(event_name, updated_task.to_dict())
