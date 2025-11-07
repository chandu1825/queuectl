# tests/test_flow.py
import subprocess
import time
import os
import json
from pathlib import Path

ROOT = Path.cwd()
CLI = ["python", "main.py"]

def run(cmd):
    print(">", " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout)
    if r.stderr:
        print("ERR:", r.stderr)
    return r

def setup_db_clean():
    # delete previous database and stop files if they exist
    p = ROOT / "jobs.db"
    if p.exists():
        p.unlink()
    stop_file = ROOT / "queuectl.stop"
    if stop_file.exists():
        stop_file.unlink()
    pid_file = ROOT / "queuectl.workers.pids"
    if pid_file.exists():
        pid_file.unlink()

def test_basic_flow():
    setup_db_clean()
    # enqueue one success and one failing job
    run(CLI + ["enqueue", '{"id":"s1","command":"echo hello"}'])
    run(CLI + ["enqueue", '{"id":"f1","command":"bash -c \\"exit 2\\"","max_retries":2}'])
    # start a worker
    p = subprocess.Popen(CLI + ["worker", "start", "--count", "1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(5)
    # show status
    run(CLI + ["status"])
    # stop workers
    run(CLI + ["worker", "stop"])
    p.terminate()
    p.wait(timeout=5)
    # show DLQ jobs
    run(CLI + ["dlq", "list"])

if __name__ == "__main__":
    test_basic_flow()
