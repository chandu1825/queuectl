import click
import json
import os
from .db import init_db, add_job, list_jobs, get_job, update_job_state, move_to_dead, get_config, set_config
from .job import job_from_json
from .worker import start_workers, stop_workers
from .db import list_jobs as db_list_jobs, get_job as db_get_job
from .db import set_config as db_set_config, get_config as db_get_config

@click.group()
def cli():
    init_db()

@cli.command()
@click.argument("job_json", type=str)
def enqueue(job_json):
    """Add a job to the queue"""
    try:
        j = job_from_json(job_json)
        add_job(j)
        click.echo(f"Enqueued job {j['id']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)

@cli.group()
def worker():
    """Worker management"""
    pass

@worker.command("start")
@click.option("--count", default=1, help="Number of worker processes to start")
@click.option("--backoff-base", default=2, help="Backoff base (exponential)")
@click.option("--timeout", default=None, type=int, help="Optional per-job timeout seconds")
def worker_start(count, backoff_base, timeout):
    """Start workers"""
    db_set_config("backoff_base", str(backoff_base))
    start_workers(count, backoff_base, timeout)

@worker.command("stop")
def worker_stop():
    """Stop workers gracefully"""
    stop_workers()
    click.echo("Stop signal sent to workers.")

@cli.command()
def status():
    """Show summary counts"""
    stats = {}
    for s in ["pending", "processing", "completed", "failed", "dead"]:
        rows = list_jobs(s)
        stats[s] = len(rows)
    active = []
    if os.path.exists("queuectl.workers.pids"):
        with open("queuectl.workers.pids", "r") as f:
            for line in f:
                try:
                    active.append(int(line.strip()))
                except:
                    pass
    click.echo(json.dumps({"stats": stats, "workers": active}, indent=2))

@cli.command()
@click.option("--state", default=None, help="Filter by job state")
def list(state):
    """List jobs"""
    rows = db_list_jobs(state)
    for r in rows:
        click.echo(json.dumps(r))

@cli.group()
def dlq():
    """Dead letter queue operations"""
    pass

@dlq.command("list")
def dlq_list():
    rows = db_list_jobs("dead")
    for r in rows:
        click.echo(json.dumps(r))

@dlq.command("retry")
@click.argument("job_id")
def dlq_retry(job_id):
    job = db_get_job(job_id)
    if not job:
        click.echo("Job not found", err=True)
        return
    if job["state"] != "dead":
        click.echo("Job is not in DLQ", err=True)
        return
    update_job_state(job_id, state="pending", attempts=0, next_run_at=None, last_error=None)
    click.echo(f"Job {job_id} retried (moved to pending)")

@cli.group()
def config():
    """Configuration get/set"""
    pass

@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    db_set_config(key, value)
    click.echo(f"config {key} set to {value}")

@config.command("get")
@click.argument("key")
def config_get(key):
    val = db_get_config(key)
    click.echo(val if val is not None else "")

def main():
    cli()

if __name__ == "__main__":
    main()
