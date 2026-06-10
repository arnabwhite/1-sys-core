import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Dict, Any, List
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.database import engine, Base, get_db, AsyncSessionLocal
from app.models import Task, TaskStatus
from app.queue_manager import enqueue_task
from app.worker import QueueWorker
from app.pubsub import pubsub

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("API")

# Initialize Queue Worker
worker = QueueWorker(concurrency=3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Auto create database tables on startup
    logger.info("Initializing database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Start the worker loop in the background
    logger.info("Starting background worker...")
    await worker.start()
    
    yield
    
    # Shutdown worker
    logger.info("Stopping worker during application shutdown...")
    await worker.stop()

app = FastAPI(
    title="AeroQueue Engine",
    description="A high-performance custom task queue with SKIP LOCKED PostgreSQL backend",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for local testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pydantic import BaseModel, Field

class CreateTaskRequest(BaseModel):
    task_type: str = Field(..., example="send_email")
    payload: Dict[str, Any] = Field(default_factory=dict, example={"email": "hello@example.com", "subject": "Welcome"})
    delay_seconds: int = Field(default=0, ge=0, example=0)
    max_retries: int = Field(default=3, ge=0, example=3)

@app.post("/api/tasks", status_code=status.HTTP_201_CREATED)
async def create_task(request: CreateTaskRequest, db: AsyncSession = Depends(get_db)):
    """
    Submit a new task to the queue.
    """
    try:
        task = await enqueue_task(
            db=db,
            task_type=request.task_type,
            payload=request.payload,
            delay_seconds=request.delay_seconds,
            max_retries=request.max_retries
        )
        task_dict = task.to_dict()
        
        # Publish enqueue event to SSE clients
        await pubsub.publish("task_enqueued", task_dict)
        
        return task_dict
    except Exception as e:
        logger.error(f"Failed to enqueue task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit task: {str(e)}"
        )

@app.get("/api/tasks")
async def list_tasks(limit: int = 50, db: AsyncSession = Depends(get_db)):
    """
    Get the list of the most recent 50 tasks.
    """
    try:
        stmt = select(Task).order_by(desc(Task.created_at)).limit(limit)
        result = await db.execute(stmt)
        tasks = result.scalars().all()
        return [t.to_dict() for t in tasks]
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving tasks from database"
        )

@app.get("/api/tasks/stream")
async def stream_tasks(request: Request):
    """
    Server-Sent Events (SSE) endpoint to receive real-time task updates.
    """
    async def event_generator():
        # Subscribe new listener
        queue = pubsub.subscribe()
        try:
            while True:
                # Check for disconnection
                if await request.is_disconnected():
                    break
                
                try:
                    # Non-blocking wait with timeout to check for disconnects
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
        finally:
            pubsub.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )

@app.get("/api/tasks/{task_id}")
async def get_task_details(task_id: str, db: AsyncSession = Depends(get_db)):
    """
    Get current details and logs for a single task.
    """
    try:
        try:
            task_uuid = uuid.UUID(task_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid UUID format"
            )
        stmt = select(Task).where(Task.id == task_uuid)
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task with ID {task_id} not found"
            )
            
        return task.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching task details: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving task details"
        )

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """
    Renders a stunning modern real-time task queue dashboard.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AeroQueue Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-primary: #0a0e17;
                --bg-secondary: #121824;
                --bg-card: #182235;
                --text-primary: #f8fafc;
                --text-secondary: #94a3b8;
                --accent-blue: #3b82f6;
                --accent-blue-glow: rgba(59, 130, 246, 0.15);
                --status-pending: #e2e8f0;
                --status-processing: #fbbf24;
                --status-completed: #10b981;
                --status-failed: #ef4444;
                --status-pending-bg: rgba(226, 232, 240, 0.1);
                --status-processing-bg: rgba(251, 191, 36, 0.1);
                --status-completed-bg: rgba(16, 185, 129, 0.1);
                --status-failed-bg: rgba(239, 68, 68, 0.1);
                --border-color: rgba(255, 255, 255, 0.06);
            }

            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }

            body {
                font-family: 'Outfit', sans-serif;
                background-color: var(--bg-primary);
                color: var(--text-primary);
                min-height: 100vh;
                padding: 2.5rem 1.5rem;
                line-height: 1.5;
            }

            .container {
                max-width: 1200px;
                margin: 0 auto;
            }

            header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2.5rem;
                border-bottom: 1px solid var(--border-color);
                padding-bottom: 1.5rem;
            }

            h1 {
                font-size: 2rem;
                font-weight: 700;
                background: linear-gradient(135deg, #60a5fa 0%, #3b82f6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }

            .connection-status {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                font-size: 0.875rem;
                font-weight: 500;
                color: var(--text-secondary);
                background: var(--bg-secondary);
                padding: 0.5rem 1rem;
                border-radius: 9999px;
                border: 1px solid var(--border-color);
            }

            .status-dot {
                width: 8px;
                height: 8px;
                background-color: var(--status-failed);
                border-radius: 50%;
                display: inline-block;
                box-shadow: 0 0 8px var(--status-failed);
            }

            .status-dot.connected {
                background-color: var(--status-completed);
                box-shadow: 0 0 8px var(--status-completed);
            }

            /* Main Layout Grid */
            .grid {
                display: grid;
                grid-template-columns: 350px 1fr;
                gap: 2rem;
            }

            @media (max-width: 900px) {
                .grid {
                    grid-template-columns: 1fr;
                }
            }

            /* Card Styling */
            .card {
                background-color: var(--bg-secondary);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
            }

            .card-title {
                font-size: 1.25rem;
                font-weight: 600;
                margin-bottom: 1.25rem;
                color: var(--text-primary);
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            /* Form Elements */
            .form-group {
                margin-bottom: 1.25rem;
            }

            label {
                display: block;
                font-size: 0.875rem;
                font-weight: 500;
                color: var(--text-secondary);
                margin-bottom: 0.5rem;
            }

            select, input, textarea {
                width: 100%;
                background-color: var(--bg-card);
                border: 1px solid var(--border-color);
                color: var(--text-primary);
                padding: 0.75rem 1rem;
                border-radius: 8px;
                font-family: inherit;
                font-size: 0.95rem;
                transition: all 0.2s ease;
            }

            select:focus, input:focus, textarea:focus {
                outline: none;
                border-color: var(--accent-blue);
                box-shadow: 0 0 0 3px var(--accent-blue-glow);
            }

            button {
                width: 100%;
                background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
                color: white;
                border: none;
                padding: 0.875rem;
                border-radius: 8px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s ease;
                margin-top: 0.5rem;
            }

            button:hover {
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
            }

            button:active {
                transform: translateY(0);
            }

            /* Task List Section */
            .task-list-container {
                display: flex;
                flex-direction: column;
                gap: 1rem;
                max-height: 600px;
                overflow-y: auto;
                padding-right: 0.5rem;
            }

            /* Custom Scrollbar */
            .task-list-container::-webkit-scrollbar {
                width: 6px;
            }
            .task-list-container::-webkit-scrollbar-track {
                background: transparent;
            }
            .task-list-container::-webkit-scrollbar-thumb {
                background: var(--border-color);
                border-radius: 10px;
            }

            .task-item {
                background-color: var(--bg-card);
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 1rem;
                display: flex;
                justify-content: space-between;
                align-items: center;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                animation: slideIn 0.3s ease-out;
            }

            .task-item.updated {
                background-color: rgba(59, 130, 246, 0.08);
                border-color: var(--accent-blue);
            }

            @keyframes slideIn {
                from { opacity: 0; transform: translateY(10px); }
                to { opacity: 1; transform: translateY(0); }
            }

            .task-info {
                display: flex;
                flex-direction: column;
                gap: 0.25rem;
            }

            .task-type-badge {
                font-size: 0.95rem;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .task-id {
                font-family: monospace;
                font-size: 0.75rem;
                color: var(--text-secondary);
            }

            .task-meta {
                font-size: 0.75rem;
                color: var(--text-secondary);
                margin-top: 0.25rem;
            }

            .task-status-container {
                display: flex;
                flex-direction: column;
                align-items: flex-end;
                gap: 0.5rem;
            }

            .status-badge {
                font-size: 0.75rem;
                font-weight: 700;
                padding: 0.25rem 0.75rem;
                border-radius: 9999px;
                text-transform: uppercase;
                letter-spacing: 0.05em;
            }

            .status-badge.pending {
                color: var(--status-pending);
                background-color: var(--status-pending-bg);
            }
            .status-badge.processing {
                color: var(--status-processing);
                background-color: var(--status-processing-bg);
                animation: pulse 1.5s infinite ease-in-out;
            }
            .status-badge.completed {
                color: var(--status-completed);
                background-color: var(--status-completed-bg);
            }
            .status-badge.failed {
                color: var(--status-failed);
                background-color: var(--status-failed-bg);
            }

            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.6; }
            }

            .retry-badge {
                font-size: 0.7rem;
                color: var(--status-processing);
                background: rgba(251, 191, 36, 0.08);
                border: 1px solid rgba(251, 191, 36, 0.2);
                border-radius: 4px;
                padding: 0.05rem 0.35rem;
            }

            .error-log {
                margin-top: 0.5rem;
                padding: 0.5rem;
                background-color: rgba(239, 68, 68, 0.05);
                border-left: 3px solid var(--status-failed);
                font-family: monospace;
                font-size: 0.725rem;
                color: #fca5a5;
                word-break: break-all;
                max-width: 100%;
                border-radius: 4px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div>
                    <h1>AeroQueue Dashboard</h1>
                    <p style="color: var(--text-secondary); font-size: 0.9rem; margin-top: 0.25rem;">
                        Real-time custom task queue running on PostgreSQL `SKIP LOCKED`
                    </p>
                </div>
                <div class="connection-status">
                    <span id="sse-dot" class="status-dot"></span>
                    <span id="sse-text">Disconnected</span>
                </div>
            </header>

            <div class="grid">
                <!-- Left Side: Control Panel -->
                <div class="card">
                    <div class="card-title">
                        <span>⚡ Trigger Task</span>
                    </div>
                    <form id="task-form">
                        <div class="form-group">
                            <label for="task-type">Task Type</label>
                            <select id="task-type">
                                <option value="send_email">✉️ Send Email</option>
                                <option value="generate_report">📊 Generate Report</option>
                                <option value="test_fail">❌ Test Failing Task</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label for="task-payload">Payload (JSON)</label>
                            <textarea id="task-payload" rows="4"></textarea>
                        </div>
                        <div class="form-group">
                            <label for="task-delay">Delay Execution (seconds)</label>
                            <input id="task-delay" type="number" value="0" min="0">
                        </div>
                        <div class="form-group">
                            <label for="task-retries">Max Retries</label>
                            <input id="task-retries" type="number" value="3" min="0">
                        </div>
                        <button type="submit">Enqueue Task</button>
                    </form>
                </div>

                <!-- Right Side: Live Monitor -->
                <div class="card">
                    <div class="card-title">
                        <span>📺 Live Monitor</span>
                    </div>
                    <div id="task-list" class="task-list-container">
                        <p id="no-tasks" style="color: var(--text-secondary); text-align: center; padding: 2rem 0;">
                            No tasks found. Submit a task to start monitoring!
                        </p>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const taskForm = document.getElementById('task-form');
            const taskTypeSelect = document.getElementById('task-type');
            const taskPayloadTextarea = document.getElementById('task-payload');
            const taskList = document.getElementById('task-list');
            const noTasksMsg = document.getElementById('no-tasks');
            const sseDot = document.getElementById('sse-dot');
            const sseText = document.getElementById('sse-text');

            // Default Payloads mapping
            const defaultPayloads = {
                send_email: { email: "user.success@example.com", subject: "Welcome to Showcase!", body: "Testing the async custom queue engine." },
                generate_report: { report_type: "financial_monthly", user_id: "usr_42a8b9" },
                test_fail: { test_purpose: "test_retries_and_failed_state" }
            };

            // Update payload when task type changes
            taskTypeSelect.addEventListener('change', () => {
                const type = taskTypeSelect.value;
                // Add conditional failure payload example for email
                if (type === 'send_email') {
                    taskPayloadTextarea.value = JSON.stringify({
                        email: "email.fail@example.com", 
                        subject: "Transient Error Test", 
                        body: "This email will cause a failure and retry."
                    }, null, 2);
                } else {
                    taskPayloadTextarea.value = JSON.stringify(defaultPayloads[type], null, 2);
                }
            });

            // Set initial default payload
            taskPayloadTextarea.value = JSON.stringify(defaultPayloads.send_email, null, 2);

            // Handle Form Submit
            taskForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                const type = taskTypeSelect.value;
                const delay = parseInt(document.getElementById('task-delay').value) || 0;
                const retries = parseInt(document.getElementById('task-retries').value) || 0;
                
                let payload = {};
                try {
                    payload = JSON.parse(taskPayloadTextarea.value);
                } catch(err) {
                    alert('Invalid JSON in payload field!');
                    return;
                }

                try {
                    const response = await fetch('/api/tasks', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            task_type: type,
                            payload: payload,
                            delay_seconds: delay,
                            max_retries: retries
                        })
                    });
                    
                    if (!response.ok) {
                        const errData = await response.json();
                        alert(`Error enqueuing task: ${errData.detail || response.statusText}`);
                    }
                } catch(err) {
                    console.error('Failed to trigger task:', err);
                    alert('Network error submitting task.');
                }
            });

            // Local cache of tasks shown on screen
            const taskCache = new Map();

            // Create or update task item in UI
            function updateTaskInUI(task) {
                if (noTasksMsg) noTasksMsg.style.display = 'none';

                let taskEl = document.getElementById(`task-${task.id}`);
                const isNew = !taskEl;

                if (isNew) {
                    taskEl = document.createElement('div');
                    taskEl.id = `task-${task.id}`;
                    taskEl.className = 'task-item';
                }

                // Add a transient flash class for visual update feedback
                taskEl.classList.add('updated');
                setTimeout(() => taskEl.classList.remove('updated'), 1000);

                const timeStr = new Date(task.created_at).toLocaleTimeString();
                const runAtStr = task.run_at ? new Date(task.run_at).toLocaleTimeString() : 'Immediate';
                
                const emoji = task.task_type === 'send_email' ? '✉️' : (task.task_type === 'generate_report' ? '📊' : '⚙️');
                
                let resultHtml = '';
                if (task.result) {
                    if (task.result.report_url) {
                        resultHtml = `
                        <div style="font-size: 0.825rem; color: var(--status-completed); margin-top: 0.25rem;">
                            Result: <a href="${task.result.report_url}" target="_blank" style="color: #60a5fa; text-decoration: underline;">Open Report 🔗</a>
                            <span style="font-size: 0.725rem; color: var(--text-secondary); display: block; font-style: italic;">
                                (Catatan: Ini adalah link S3 simulasi dari task queue!)
                            </span>
                        </div>
                        `;
                    } else {
                        resultHtml = `
                        <div style="font-size: 0.825rem; color: var(--status-completed); margin-top: 0.25rem;">
                            Result: <code style="color: #34d399">${JSON.stringify(task.result)}</code>
                        </div>
                        `;
                    }
                }

                let errorHtml = '';
                if (task.status === 'FAILED' && task.error_message) {
                    errorHtml = `<div class="error-log"><strong>Error:</strong> ${task.error_message.split('\\n')[0]}</div>`;
                } else if (task.status === 'PENDING' && task.retry_count > 0) {
                    errorHtml = `<div class="error-log" style="border-left-color: var(--status-processing); color: #fde047;">
                        <strong>Last Error:</strong> ${task.error_message ? task.error_message.split('\\n')[0] : 'Retry Scheduled'}
                    </div>`;
                }

                taskEl.innerHTML = `
                    <div class="task-info">
                        <div class="task-type-badge">
                            <span>${emoji} ${task.task_type}</span>
                            ${task.retry_count > 0 ? `<span class="retry-badge">Retry ${task.retry_count}/${task.max_retries}</span>` : ''}
                        </div>
                        <div class="task-id">ID: ${task.id}</div>
                        <div class="task-meta">
                            Created: ${timeStr} | Run At: ${runAtStr}
                        </div>
                        <div style="font-size: 0.8rem; color: var(--text-secondary); margin-top: 0.25rem;">
                            Payload: <code style="color: #60a5fa">${JSON.stringify(task.payload)}</code>
                        </div>
                        ${resultHtml}
                        ${errorHtml}
                    </div>
                    <div class="task-status-container">
                        <span class="status-badge ${task.status.toLowerCase()}">${task.status}</span>
                    </div>
                `;

                if (isNew) {
                    taskList.insertBefore(taskEl, taskList.firstChild);
                }
                
                taskCache.set(task.id, task);
            }

            // Load Existing Tasks
            async function loadTasks() {
                try {
                    const response = await fetch('/api/tasks');
                    if (response.ok) {
                        const tasks = await response.json();
                        if (tasks.length > 0 && noTasksMsg) noTasksMsg.style.display = 'none';
                        // Add oldest to newest so they stack correctly in order of newest-first
                        tasks.reverse().forEach(task => updateTaskInUI(task));
                    }
                } catch(err) {
                    console.error('Error fetching tasks initial load:', err);
                }
            }

            // Initialize EventSource for real-time updates
            function startSSE() {
                const sse = new EventSource('/api/tasks/stream');

                sse.onopen = () => {
                    sseDot.className = 'status-dot connected';
                    sseText.innerText = 'Live Streaming Connected';
                    sseText.style.color = 'var(--status-completed)';
                };

                sse.onerror = (err) => {
                    console.error('SSE Error:', err);
                    sseDot.className = 'status-dot';
                    sseText.innerText = 'Reconnecting...';
                    sseText.style.color = 'var(--status-failed)';
                };

                // Helper for all event types
                const handleEvent = (event) => {
                    try {
                        const taskData = JSON.parse(event.data);
                        updateTaskInUI(taskData);
                    } catch(err) {
                        console.error('Error handling SSE event data:', err);
                    }
                };

                sse.addEventListener('task_enqueued', handleEvent);
                sse.addEventListener('task_started', handleEvent);
                sse.addEventListener('task_completed', handleEvent);
                sse.addEventListener('task_failed', handleEvent);
                sse.addEventListener('task_retrying', handleEvent);
            }

            // Init
            loadTasks().then(startSSE);
        </script>
    </body>
    </html>
    """
    return html_content
