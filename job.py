"""
job.py
======
The unit of work in the queue: a Job. A job carries the function to run (by
registered name), its arguments, scheduling metadata (priority, when it may run),
and reliability metadata (retry policy, attempt count, state).

Design choices
--------------
* Jobs are identified by a stable string id so they can be tracked across the
  queue, the worker pool, and the persistence log.
* A job is referenced by a REGISTERED TASK NAME rather than a raw callable, so it
  can be serialized to disk and reconstructed on recovery (you cannot pickle an
  arbitrary function reliably across processes/restarts).
* Priority is an integer where LOWER runs first (a min-heap convention), matching
  how the scheduler orders work.
* run_at supports delayed / scheduled execution: a job is not eligible until the
  clock passes run_at.
"""

import time
import uuid
import enum
from dataclasses import dataclass, field, asdict
from typing import Any


class JobState(str, enum.Enum):
    PENDING = "pending"       # in the queue, not yet started
    RUNNING = "running"       # picked up by a worker
    SUCCEEDED = "succeeded"
    FAILED = "failed"         # exhausted all retries
    RETRYING = "retrying"     # failed once, scheduled to run again
    DEAD = "dead"             # moved to the dead-letter queue


@dataclass(order=False)
class Job:
    task_name: str
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    priority: int = 100            # lower = more important
    max_retries: int = 3
    retry_backoff: float = 0.5     # base seconds for exponential backoff
    run_at: float = field(default_factory=time.time)  # earliest eligible time

    # runtime state (not set by the caller)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: JobState = JobState.PENDING
    attempts: int = 0
    last_error: str = ""
    result: Any = None
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["state"] = self.state.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        d = dict(d)
        d["state"] = JobState(d.get("state", "pending"))
        d["args"] = tuple(d.get("args", ()))
        return cls(**d)

    def next_backoff(self) -> float:
        """Exponential backoff delay for the current attempt count."""
        return self.retry_backoff * (2 ** max(0, self.attempts - 1))
