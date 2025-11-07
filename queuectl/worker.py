# queuectl/worker.py
import subprocess
import time
import os
import signal
from multiprocessing import Process
from typing import Optional
from .db import claim_job, update_job_state, move_to_dead
from .job import compute_backoff

STOP_FLAG_FILE = os.path.join(os.getcwd(), "queuectl.stop")

def worker_loop(worker_name: str, base_backoff: int = 2, default_timeout: Optional[int]=None):
    print(f"[worker {worker_name}] starting loop (base_backoff={base_backoff})")
    while True:
        if os.path.exists(STOP_FLAG_FILE):
            print(f"[worker {worker_name}] stop flag detected, exiting loop.")
            break
        job = claim_job(worker_name)
        if not job:
            time.sleep(1)
            continue
        job_id = job["id"]
        command = job["command"]
        print(f"[worker {worker_name}] claimed job {job_id}: {command}")
        try:
            completed = subprocess.run(command, shell=True, capture_output=True, timeout=default_timeout)
            rc = completed.returncode
            stdout = (completed.stdout or b"").decode(errors="ignore")
            stderr = (completed.stderr or b"").decode(errors="ignore")
            if rc == 0:
                update_job_state(job_id, state="completed", attempts=job["attempts"], last_error=stdout or None)
                print(f"[worker {worker_name}] job {job_id} completed.")
            else:
                attempts = job["attempts"] + 1
                max_retries = job.get("max_retries", 3)
                errtxt = f"rc={rc}; stdout={stdout}; stderr={stderr}"
                if attempts > max_retries:
                    move_to_dead(job_id, last_error=errtxt)
                    print(f"[worker {worker_name}] job {job_id} moved to DLQ.")
                else:
                    delay = compute_backoff(base_backoff, attempts)
                    from datetime import datetime, timezone
                    next_run_at = datetime.fromtimestamp(time.time() + delay, tz=timezone.utc).isoformat()
                    update_job_state(job_id, state="pending", attempts=attempts, next_run_at=next_run_at, last_error=errtxt)
                    print(f"[worker {worker_name}] job {job_id} failed, will retry after {delay}s.")
        except Exception as e:
            attempts = job["attempts"] + 1
            errtxt = f"exception: {str(e)}"
            max_retries = job.get("max_retries", 3)
            if attempts > max_retries:
                move_to_dead(job_id, last_error=errtxt)
                print(f"[worker {worker_name}] job {job_id} exception -> DLQ.")
            else:
                delay = compute_backoff(base_backoff, attempts)
                from datetime import datetime, timezone
                next_run_at = datetime.fromtimestamp(time.time() + delay, tz=timezone.utc).isoformat()
                update_job_state(job_id, state="pending", attempts=attempts, next_run_at=next_run_at, last_error=errtxt)
                print(f"[worker {worker_name}] job {job_id} exception, retry after {delay}s.")

def start_workers(count: int = 1, base_backoff: int = 2, timeout: Optional[int]=None):
    procs = []
    for i in range(count):
        name = f"w-{os.getpid()}-{i}"
        p = Process(target=worker_loop, args=(name, base_backoff, timeout), daemon=False)
        p.start()
        procs.append(p)
        print(f"[master] started worker pid={p.pid} name={name}")
    with open("queuectl.workers.pids", "w") as f:
        for p in procs:
            f.write(str(p.pid) + "\n")
    print("[master] all workers started.")
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("[master] received interrupt, creating stop flag...")
        open(STOP_FLAG_FILE, "w").close()
        for p in procs:
            p.join(timeout=10)
        print("[master] shutdown complete.")

def stop_workers():
    open(STOP_FLAG_FILE, "w").close()
    print("[master] stop flag created. Workers will exit when current job completes.")
    if os.path.exists("queuectl.workers.pids"):
        with open("queuectl.workers.pids", "r") as f:
            for line in f:
                try:
                    pid = int(line.strip())
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
