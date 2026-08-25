"""
scheduler.py
============
The task queue engine: a thread-pool of workers pulling jobs from the delay-aware
priority queue, executing registered tasks, and handling failure with retries,
exponential backoff, a dead-letter queue, and durable journaling.

Reliability model
-----------------
* At-least-once execution: a job is journaled before and after execution; a job
  interrupted by a crash is replayed and retried on restart.
* Retries with exponential backoff: a failed job is re-enqueued with run_at set
  into the future (backoff grows per attempt) until max_retries is exhausted,
  after which it moves to the dead-letter queue.
* Graceful shutdown: shutdown() stops accepting new work, lets in-flight jobs
  finish, and joins all workers cleanly (no lost or half-done jobs).

Concurrency
-----------
Many worker threads share one DelayPriorityQueue; its condition variable lets
idle workers block without busy-waiting. Task functions are registered by name so
jobs stay serializable for persistence.
"""

import time
import threading
import traceback

from .job import Job, JobState
from .priority_queue import DelayPriorityQueue
from .persistence import Journal, DeadLetterQueue


class TaskQueue:
    def __init__(self, num_workers: int = 4, journal_path: str = "data/journal.log",
                 dlq_path: str = "data/dead_letter.log", recover: bool = False):
        self.registry = {}                      # task_name -> callable
        self.queue = DelayPriorityQueue()
        self.journal = Journal(journal_path)
        self.dlq = DeadLetterQueue(dlq_path)
        self.num_workers = num_workers
        self._workers = []
        self._running = False
        self._accepting = True
        self._lock = threading.Lock()

        # metrics
        self.stats = {"submitted": 0, "succeeded": 0, "failed": 0,
                      "retried": 0, "dead": 0}

        if recover:
            self._recover(journal_path)

    # --- registration ---

    def task(self, name=None):
        """Decorator to register a task function by name."""
        def deco(fn):
            self.registry[name or fn.__name__] = fn
            return fn
        return deco

    def register(self, name, fn):
        self.registry[name] = fn

    # --- submission ---

    def submit(self, task_name, *args, priority=100, max_retries=3,
               delay=0.0, **kwargs) -> str:
        """Enqueue a job. Returns its id. `delay` schedules it into the future."""
        if not self._accepting:
            raise RuntimeError("queue is shutting down; not accepting new jobs")
        if task_name not in self.registry:
            raise KeyError(f"unknown task '{task_name}'")
        job = Job(task_name=task_name, args=args, kwargs=kwargs,
                  priority=priority, max_retries=max_retries,
                  run_at=time.time() + delay)
        with self._lock:
            self.stats["submitted"] += 1
        self.journal.record(job)
        self.queue.put(job)
        return job.id

    # --- lifecycle ---

    def start(self):
        self._running = True
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker_loop, name=f"worker-{i}",
                                 daemon=True)
            t.start()
            self._workers.append(t)

    def shutdown(self, wait=True, timeout=30):
        """Stop accepting new jobs, drain in-flight work, join workers."""
        self._accepting = False
        # Wait for the queue to drain if requested.
        if wait:
            deadline = time.time() + timeout
            while len(self.queue) > 0 and time.time() < deadline:
                time.sleep(0.02)
        self._running = False
        self.queue.close()
        for t in self._workers:
            t.join(timeout=timeout)
        self.journal.close()

    # --- worker loop ---

    def _worker_loop(self):
        while self._running:
            job = self.queue.get(timeout=0.2)
            if job is None:
                continue
            self._execute(job)

    def _execute(self, job: Job):
        job.state = JobState.RUNNING
        job.attempts += 1
        self.journal.record(job)
        fn = self.registry.get(job.task_name)
        try:
            job.result = fn(*job.args, **job.kwargs)
            job.state = JobState.SUCCEEDED
            with self._lock:
                self.stats["succeeded"] += 1
            self.journal.record(job)
        except Exception as exc:                       # noqa: BLE001
            job.last_error = f"{type(exc).__name__}: {exc}"
            if job.attempts <= job.max_retries:
                job.state = JobState.RETRYING
                job.run_at = time.time() + job.next_backoff()
                with self._lock:
                    self.stats["retried"] += 1
                self.journal.record(job)
                self.queue.put(job)                    # re-enqueue with backoff
            else:
                job.state = JobState.DEAD
                with self._lock:
                    self.stats["failed"] += 1
                    self.stats["dead"] += 1
                self.journal.record(job)
                self.dlq.add(job)

    # --- recovery ---

    def _recover(self, journal_path):
        outstanding = Journal.replay(journal_path)
        for job in outstanding:
            self.queue.put(job)
        return len(outstanding)

    def wait_until_idle(self, timeout=30):
        """Block until the queue is empty (best-effort, for tests/examples)."""
        deadline = time.time() + timeout
        while len(self.queue) > 0 and time.time() < deadline:
            time.sleep(0.02)


if __name__ == "__main__":
    import tempfile, os
    d = tempfile.mkdtemp()
    tq = TaskQueue(num_workers=4, journal_path=os.path.join(d, "j.log"),
                   dlq_path=os.path.join(d, "dlq.log"))

    results = []
    @tq.task("add")
    def add(a, b):
        results.append(a + b)
        return a + b

    @tq.task("flaky")
    def flaky():
        raise ValueError("always fails")

    tq.start()
    for i in range(10):
        tq.submit("add", i, i, priority=10)
    tq.submit("flaky", max_retries=2)
    tq.wait_until_idle()
    time.sleep(1.0)   # let retries/backoff settle
    tq.shutdown()
    print("results:", sorted(results))
    print("stats:", tq.stats)
    print("dead-letter jobs:", len(tq.dlq))
