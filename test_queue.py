import asyncio
import httpx
import time
import sys

BASE_URL = "http://localhost:8000"

async def trigger_task(client: httpx.AsyncClient, task_type: str, payload: dict, delay_seconds: int = 0):
    url = f"{BASE_URL}/api/tasks"
    data = {
        "task_type": task_type,
        "payload": payload,
        "delay_seconds": delay_seconds,
        "max_retries": 2
    }
    try:
        response = await client.post(url, json=data)
        if response.status_code == 201:
            task = response.json()
            print(f"[Triggered] {task_type} -> Task ID: {task['id']} (Status: {task['status']})")
            return task['id']
        else:
            print(f"[Failed to Trigger] {task_type}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[Trigger Error] {task_type}: {e}")
        return None

async def poll_tasks(client: httpx.AsyncClient, task_ids: list):
    print("\n--- Starting Poll Monitoring ---")
    start_time = time.time()
    
    # Poll until all tasks are either COMPLETED or FAILED
    while True:
        completed_count = 0
        failed_count = 0
        processing_count = 0
        pending_count = 0
        
        for task_id in task_ids:
            if not task_id:
                continue
            try:
                response = await client.get(f"{BASE_URL}/api/tasks/{task_id}")
                if response.status_code == 200:
                    task = response.json()
                    status = task['status']
                    if status == "COMPLETED":
                        completed_count += 1
                    elif status == "FAILED":
                        failed_count += 1
                    elif status == "PROCESSING":
                        processing_count += 1
                    elif status == "PENDING":
                        pending_count += 1
            except Exception as e:
                print(f"[Poll Error] Task {task_id}: {e}")

        total_done = completed_count + failed_count
        elapsed = time.time() - start_time
        print(f"[{elapsed:.1f}s] Pending: {pending_count} | Processing: {processing_count} | Completed: {completed_count} | Failed: {failed_count} (Total Done: {total_done}/{len(task_ids)})")

        if total_done == len(task_ids):
            print(f"\n✨ All tasks finished in {elapsed:.2f} seconds!")
            break
            
        await asyncio.sleep(1.0)

async def main():
    print("🚀 AeroQueue Stress Test")
    print(f"Connecting to API at {BASE_URL}...")
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Check if server is running
        try:
            r = await client.get(f"{BASE_URL}/")
            if r.status_code != 200:
                print("Server is up but returned non-200 status.")
                sys.exit(1)
        except Exception:
            print("❌ Error: API Server is not running! Make sure to start the server (or docker-compose) first.")
            sys.exit(1)

        # We will trigger a mix of:
        # - 20 Successful emails (simulate sending emails, 2.0s delay each)
        # - 5 Failing emails to email.fail@example.com (tests transient error retries)
        # - 10 Reports (simulate reporting, 4.0s delay each)
        # - 5 Test fails (designed to fail immediately, retries, and FAILED status)
        tasks_to_trigger = []
        
        print("\nTriggering 40 concurrent tasks to stress test locking & queue worker limit (concurrency=3)...")
        
        # 20 Success Emails
        for i in range(20):
            tasks_to_trigger.append(
                trigger_task(client, "send_email", {"email": f"user.{i}@success.com", "subject": f"Welcome #{i}"})
            )
            
        # 5 Transient Fail Emails (triggers retry)
        for i in range(5):
            tasks_to_trigger.append(
                trigger_task(client, "send_email", {"email": "email.fail@example.com", "subject": f"Fail Test #{i}"})
            )
            
        # 10 Reports
        for i in range(10):
            tasks_to_trigger.append(
                trigger_task(client, "generate_report", {"report_type": "summary", "user_id": f"usr_{i}"})
            )
            
        # 5 Hard Fails
        for i in range(5):
            tasks_to_trigger.append(
                trigger_task(client, "test_fail", {"reason": "check_hard_fail"})
            )

        # Trigger all posts concurrently
        task_ids = await asyncio.gather(*tasks_to_trigger)
        # Filter out failed enqueues
        task_ids = [tid for tid in task_ids if tid is not None]

        print(f"\nSuccessfully enqueued {len(task_ids)} tasks.")
        
        # Start polling
        await poll_tasks(client, task_ids)

if __name__ == "__main__":
    asyncio.run(main())
