"""
persistence.py
==============
A durability layer so jobs survive a crash. Every state transition a job goes
through (enqueued, started, succeeded, failed, retrying, dead) is appended to an
on-disk journal as a JSON line. On restart, the journal is replayed to rebuild
the set of jobs that were still outstanding, so no accepted job is silently lost.

Why append-only (a log)?
------------------------
Appending is fast and crash-safe: a partially written final line is simply
ignored on replay (we skip lines that don't parse), so a crash mid-write cannot
corrupt earlier records. This is the same write-ahead principle databases use.

Replay logic
------------
We fold the log into a final state per job id: the LAST record for each id wins.
Jobs whose final state is terminal (succeeded / dead) are dropped; jobs left
pending / retrying / running are considered outstanding and are requeued (a job
that was 'running' when the process died is retried, i.e. at-least-once delivery).
"""

import os
import json
import threading


class Journal:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._f = open(path, "a", buffering=1)   # line-buffered

    def record(self, job):
        """Append the job's current state as one JSON line."""
        line = json.dumps(job.to_dict(), separators=(",", ":"))
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()
            os.fsync(self._f.fileno())

    def close(self):
        with self._lock:
            self._f.close()

    @staticmethod
    def replay(path: str):
        """Return the list of OUTSTANDING jobs to requeue after a restart."""
        from .job import Job, JobState
        if not os.path.exists(path):
            return []
        latest = {}   # job_id -> most recent dict
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue   # torn final line from a crash -> ignore
                latest[d["id"]] = d

        outstanding = []
        terminal = {JobState.SUCCEEDED.value, JobState.DEAD.value}
        for d in latest.values():
            if d.get("state") not in terminal:
                job = Job.from_dict(d)
                # A job caught mid-flight is retried (at-least-once).
                if job.state in (JobState.RUNNING, JobState.RETRYING):
                    job.state = JobState.PENDING
                outstanding.append(job)
        return outstanding


class DeadLetterQueue:
    """Jobs that exhausted their retries land here for inspection, instead of
    vanishing. A separate journal keeps them auditable."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._jobs = []

    def add(self, job):
        with self._lock:
            self._jobs.append(job)
            with open(self.path, "a") as f:
                f.write(json.dumps(job.to_dict(), separators=(",", ":")) + "\n")

    def all(self):
        with self._lock:
            return list(self._jobs)

    def __len__(self):
        with self._lock:
            return len(self._jobs)
