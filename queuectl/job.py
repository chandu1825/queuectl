# queuectl/job.py
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import json

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def compute_backoff(base: int, attempts: int) -> int:
    return max(1, base ** attempts)

def job_from_json(s: str) -> Dict[str, Any]:
    data = json.loads(s)
    if "id" not in data or "command" not in data:
        raise ValueError("job JSON must include 'id' and 'command'")
    data.setdefault("state", "pending")
    data.setdefault("attempts", 0)
    data.setdefault("max_retries", 3)
    data.setdefault("created_at", now_iso())
    data.setdefault("updated_at", now_iso())
    return data
