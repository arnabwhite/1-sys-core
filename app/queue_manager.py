import datetime
from typing import Optional, Dict, Any
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Task, TaskStatus

async def enqueue_task(
    db: AsyncSession, 
    task_type: str, 
    payload: Dict[str, Any], 
    delay_seconds: int = 0, 
    max_retries: int = 3
) -> Task:
    """
    Enqueue a new task into the database queue.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    run_at = now
    if delay_seconds > 0:
        run_at = now + datetime.timedelta(seconds=delay_seconds)
        
    task = Task(
        task_type=task_type,
        payload=payload,
        status=TaskStatus.PENDING,
        max_retries=max_retries,
        run_at=run_at,
        created_at=now,
        updated_at=now
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    return task

async def dequeue_task(db: AsyncSession) -> Optional[Task]:
    """
    Atomically retrieve the next pending task using Postgres 'SELECT ... FOR UPDATE SKIP LOCKED'.
    Updates status to PROCESSING before releasing lock.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Select the oldest pending task that is ready to run
    # Skip locked rows to avoid blocking concurrent workers
    stmt = (
        select(Task)
        .where(
            Task.status == TaskStatus.PENDING,
            Task.run_at <= now
        )
        .order_by(Task.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()
    
    if task:
        task.status = TaskStatus.PROCESSING
        task.updated_at = now
        await db.commit()
        await db.refresh(task)
        return task
        
    return None

async def complete_task(db: AsyncSession, task_id: Any, result: Optional[Dict[str, Any]] = None) -> Task:
    """
    Mark a task as completed.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    stmt = select(Task).where(Task.id == task_id)
    db_result = await db.execute(stmt)
    task = db_result.scalar_one()
    
    task.status = TaskStatus.COMPLETED
    task.updated_at = now
    task.error_message = None
    task.result = result
    await db.commit()
    await db.refresh(task)
    return task

async def fail_task(db: AsyncSession, task_id: Any, error_msg: str) -> Task:
    """
    Handle task failure. Increments retry counter. If retries remain, schedules
    re-execution with exponential backoff. Otherwise, marks task as FAILED.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    stmt = select(Task).where(Task.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one()
    
    task.retry_count += 1
    task.error_message = error_msg
    task.updated_at = now
    
    if task.retry_count <= task.max_retries:
        # Exponential backoff: retry after 2, 4, 8, 16... seconds
        backoff_seconds = 2 ** task.retry_count
        task.status = TaskStatus.PENDING
        task.run_at = now + datetime.timedelta(seconds=backoff_seconds)
    else:
        task.status = TaskStatus.FAILED
        
    await db.commit()
    await db.refresh(task)
    return task
