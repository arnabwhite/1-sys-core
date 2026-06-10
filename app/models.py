import uuid
from datetime import datetime
from enum import Enum as PyEnum
from typing import Any, Dict, Optional
from sqlalchemy import String, Integer, DateTime, JSON, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class TaskStatus(str, PyEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, 
        default=uuid.uuid4
    )
    task_type: Mapped[str] = mapped_column(
        String(100), 
        nullable=False, 
        index=True
    )
    payload: Mapped[Dict[str, Any]] = mapped_column(
        JSON, 
        nullable=False, 
        default=dict
    )
    status: Mapped[TaskStatus] = mapped_column(
        String(20), 
        nullable=False, 
        default=TaskStatus.PENDING,
        index=True
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, 
        nullable=False, 
        default=0
    )
    max_retries: Mapped[int] = mapped_column(
        Integer, 
        nullable=False, 
        default=3
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text, 
        nullable=True
    )
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True
    )
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=func.now(),
        index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=func.now(), 
        onupdate=func.now()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "task_type": self.task_type,
            "payload": self.payload,
            "status": self.status.value if isinstance(self.status, TaskStatus) else self.status,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "result": self.result,
            "run_at": self.run_at.isoformat() if self.run_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
