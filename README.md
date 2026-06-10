# ⚡ AeroQueue: Custom Distributed Task Queue

A highly-scalable, modular, and performant custom distributed task queue engine built in **Python (FastAPI + Asyncio)** with **PostgreSQL** as the message broker. 

This project demonstrates how task queue systems (like Celery or BullMQ) operate under the hood by implementing **row locking** and **concurrency control** from scratch.

---

## 🛠️ Tech Stack & Concepts Demonstrated

- **Python 3.11** with **Asyncio**: High-performance asynchronous polling and task execution.
- **FastAPI**: API endpoints to ingest tasks, monitor state, and stream updates.
- **PostgreSQL (Neon)**: Database engine used as a lightweight, transactional message broker.
- **SQLAlchemy 2.0 (Async)**: Modern async ORM pattern with connection pooling.
- **Server-Sent Events (SSE)**: Streams real-time task status changes directly to the UI dashboard.
- **Docker & Docker Compose**: Local containerized development.

---

## 🧠 System Design & Concurrency: Under the Hood

### 1. How PostgreSQL behaves as a Broker (`SKIP LOCKED`)
In a distributed queue, multiple worker instances poll the database for new tasks. Without protection, two workers could fetch the same task at the same millisecond, leading to duplicate executions.

To prevent this race condition, we use PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` row locking:
```sql
SELECT * FROM tasks
WHERE status = 'PENDING' AND run_at <= NOW()
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```
* **`FOR UPDATE`**: Locks the row(s) returned by the query. No other transaction can modify or lock these rows until this transaction commits.
* **`SKIP LOCKED`**: If another worker has already locked a row, instead of waiting for the lock to release (blocking the thread), this query simply skips that row and finds the next unlocked task. 

This enables lock-free, highly parallel dequeueing!

### 2. Multi-Process vs Single-Process Deployment
- **Production/Scale (Local Docker-Compose)**: The API Gateway and the Workers run as separate containers. You can spin up 5 worker containers, and they will safely share the queue using `SKIP LOCKED`.
- **Render Free Tier Integration**: Since Render charges per background worker container, we run the `QueueWorker` loop inside the same FastAPI process using `asyncio.create_task` at startup. The database transactions remain isolated, keeping the app modular and free-to-host.

### 3. Resilient Retries & Exponential Backoff
When a task handler raises an exception, the worker catches it and checks `retry_count`. If `retry_count < max_retries`:
1. Increments `retry_count`.
2. Leaves status as `PENDING`.
3. Calculates `run_at = now + (2 ** retry_count) seconds` (exponential backoff: 2s, 4s, 8s, 16s...).
This prevents overloading downstream APIs that might be down temporarily.

---

## 🚀 How to Run Locally

### Option A: Using Docker Compose (Recommended)
This starts both the FastAPI app and a local PostgreSQL database container:

1. Build and start the services:
   ```bash
   docker-compose up --build
   ```
2. Open your browser and navigate to:
   **[http://localhost:8000/](http://localhost:8000/)** to access the live dashboard.
3. Run the stress test script in another terminal to see concurrent queue execution:
   ```bash
   pip install httpx
   python test_queue.py
   ```   

### Option B: Local Virtual Environment
If you want to run it without Docker, you will need a running PostgreSQL database:

1. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Set your database environment variable:
   ```bash
   export DATABASE_URL="postgresql+asyncpg://<username>:<password>@localhost:5432/<dbname>"
   ```
3. Run the FastAPI application:
   ```bash
   uvicorn app.main:app --reload
   ```

---

## ☁️ Deployment Guide (Neon + Render Free Tier)

### 1. Database Setup (Neon.tech)
1. Go to **[Neon.tech](https://neon.tech/)** and create a free account.
2. Create a new project and select **PostgreSQL**.
3. Copy the **Connection String** from the dashboard. It will look like:
   `postgres://alex:pwd@ep-flat-water-12345.us-east-2.aws.neon.tech/neondb?sslmode=require`

### 2. API & Worker Setup (Render.com)
1. Register/Login to **[Render.com](https://render.com/)**.
2. Click **New +** and select **Web Service**.
3. Connect your GitHub repository containing this project folder.
4. Set the following details:
   - **Language**: `Docker` (Render will automatically detect the `Dockerfile` at the root)
   - **Plan**: `Free`
5. In the **Environment Variables** section, add:
   - `DATABASE_URL`: *Paste the connection string from Neon.tech*
6. Click **Deploy Web Service**.
7. Once deployed, visit the URL provided by Render to open your live queue engine dashboard!
