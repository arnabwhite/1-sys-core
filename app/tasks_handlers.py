import asyncio
import logging
import random
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TaskHandlers")

async def handle_send_email(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates sending an email.
    """
    email = payload.get("email", "unknown@example.com")
    subject = payload.get("subject", "No Subject")
    
    logger.info(f"[Email Task] Starting to send email to {email} with subject: '{subject}'")
    
    # Simulate network delay
    await asyncio.sleep(2.0)
    
    # Simulate random transient network error for email.fail@example.com to test retries
    if email == "email.fail@example.com":
        logger.warning(f"[Email Task] Simulated transient error sending email to {email}")
        raise ConnectionError("SMTP server handshake timed out (simulated).")
        
    logger.info(f"[Email Task] Email successfully sent to {email}")
    return {"status": "sent", "recipient": email}

async def handle_generate_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates a heavy reporting job.
    """
    report_type = payload.get("report_type", "summary")
    user_id = payload.get("user_id", "guest")
    
    logger.info(f"[Report Task] Generating '{report_type}' report for user: {user_id}")
    
    # Simulate DB querying & PDF generation
    await asyncio.sleep(4.0)
    
    logger.info(f"[Report Task] Report generation complete for user: {user_id}")
    return {"status": "generated", "report_url": f"https://s3.amazonaws.com/reports/{user_id}/{random.randint(1000, 9999)}.pdf"}

async def handle_test_fail(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates a task that always fails.
    """
    logger.info("[Test Fail Task] Attempting execution...")
    await asyncio.sleep(1.0)
    raise ValueError("This task is designed to fail and trigger retries.")

# Registry mapping task_type to its handler function
TASK_HANDLERS = {
    "send_email": handle_send_email,
    "generate_report": handle_generate_report,
    "test_fail": handle_test_fail
}

async def execute_task(task_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute task by calling registered handler.
    """
    if task_type not in TASK_HANDLERS:
        raise ValueError(f"Unknown task type: '{task_type}'")
    
    handler = TASK_HANDLERS[task_type]
    return await handler(payload)
