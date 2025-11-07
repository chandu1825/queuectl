QueueCTL — CLI-Based Background Job Queue System



QueueCTL is a command-line based background job management system built in Python.

It executes background jobs, manages multiple workers, retries failed jobs automatically with exponential backoff, and moves permanently failed jobs to a Dead Letter Queue (DLQ) for review or retry.



Features



Enqueue background jobs via CLI



Run multiple worker processes



Retry failed jobs with exponential backoff (delay = base^attempts)



Move permanently failed jobs to Dead Letter Queue (DLQ)



Graceful worker shutdown



Persistent storage using SQLite



Configurable retry count and backoff base



Clean, modular code with separate components



CLI built with click for user-friendly commands



Tech Stack



Language: Python 3.10+



Framework: Click (for CLI)



Database: SQLite (persistent job storage)



Concurrency: Python multiprocessing



Platform: Windows 10 (tested)



Setup Instructions (Windows)

1\. Clone and open the project

git clone https://github.com/chandu1825/queuectl.git

cd queuectl



2\. Create and activate virtual environment

python -m venv venv

venv\\Scripts\\activate.bat



3\. Install dependencies

pip install -r requirements.txt



4\. Verify installation

python main.py status



CLI Usage Examples

Enqueue a successful job

python main.py enqueue "{\\"id\\":\\"success1\\",\\"command\\":\\"cmd /c echo Hello QueueCTL!\\",\\"max\_retries\\":2}"



Enqueue a failing job

python main.py enqueue "{\\"id\\":\\"fail1\\",\\"command\\":\\"cmd /c exit 2\\",\\"max\_retries\\":2}"



Start a worker

python main.py worker start --count 1 --backoff-base 2



View DLQ

python main.py dlq list



Retry DLQ job

python main.py dlq retry fail1



Stop workers gracefully



Press Ctrl + C



Architecture Overview



QueueCTL Components



Component	Description

CLI (main.py \& cli.py)	Handles all commands via click

Database (db.py)	Stores all jobs, states, errors, and configs persistently

Worker System (worker.py)	Executes jobs concurrently using multiprocessing

Retry \& Backoff (job.py)	Retries failed jobs after delay = base^attempts

Dead Letter Queue (DLQ)	Stores permanently failed jobs for later review

Configuration (config.py)	Manages retry limits and backoff parameters

Job Lifecycle

State	Description

pending	Waiting to be picked by a worker

processing	Currently executing

completed	Executed successfully

failed	Failed, will retry

dead	Permanently failed (moved to DLQ)

Design Decisions \& Trade-offs



Used SQLite for simplicity (no external dependencies).



Focused on CLI for robustness and clarity.



Implemented timeout handling, retry, and logging for reliability.



Could be extended later with a web dashboard, job priorities, or scheduled jobs.



Demo Video



Demo Recording Steps:



Activate venv



Enqueue jobs



Start worker



Check DLQ and retry failed jobs



View final status



Demonstrate persistence



Demo Link: https://drive.google.com/file/d/1PhSe6QP97yY0kYBw0nPuElzMS\_dtyD-n/view?usp=drive\_link



Testing Instructions



Run the automated flow:



python tests\\test\_flow.py





It verifies enqueue, worker, retry, and DLQ behavior.

