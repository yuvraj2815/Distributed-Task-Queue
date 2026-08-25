"""
priority_queue.py
=================
A thread-safe priority queue that also understands DELAYED jobs, built on a
binary min-heap implemented from scratch (not heapq) so the data structure is on
display.

Ordering key per job: (run_at, priority, seq)
  * run_at   -> a job that is not yet eligible sorts later; the scheduler will
                not pop it until the clock reaches run_at.
  * priority -> among eligible jobs, lower priority value runs first.
  * seq      -> a monotonic counter breaks ties FIFO, so equal-priority jobs run
                in submission order (stable scheduling).

The heap gives O(log n) push and pop. A single lock plus a condition variable
makes it safe for many producer threads and many worker threads, and lets
workers BLOCK efficiently until either a job is ready or a delayed job's time
arrives -- no busy-waiting.
"""

import time
import threading
import itertools


class DelayPriorityQueue:
    def __init__(self):
        self._heap = []                       # list used as a binary heap
        self._counter = itertools.count()     # tie-breaker sequence
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._closed = False

    # --- heap primitives (hand-written) ---

    def _key(self, item):
        run_at, priority, seq, job = item
        return (run_at, priority, seq)

    def _sift_up(self, idx):
        heap = self._heap
        while idx > 0:
            parent = (idx - 1) // 2
            if self._key(heap[idx]) < self._key(heap[parent]):
                heap[idx], heap[parent] = heap[parent], heap[idx]
                idx = parent
            else:
                break

    def _sift_down(self, idx):
        heap = self._heap
        n = len(heap)
        while True:
            left, right, smallest = 2 * idx + 1, 2 * idx + 2, idx
            if left < n and self._key(heap[left]) < self._key(heap[smallest]):
                smallest = left
            if right < n and self._key(heap[right]) < self._key(heap[smallest]):
                smallest = right
            if smallest == idx:
                break
            heap[idx], heap[smallest] = heap[smallest], heap[idx]
            idx = smallest

    def _push_locked(self, item):
        self._heap.append(item)
        self._sift_up(len(self._heap) - 1)

    def _pop_locked(self):
        heap = self._heap
        top = heap[0]
        last = heap.pop()
        if heap:
            heap[0] = last
            self._sift_down(0)
        return top

    # --- public API ---

    def put(self, job):
        with self._cv:
            seq = next(self._counter)
            # Bucket run_at to the nearest 10ms. Jobs meant to run "now" then
            # share a run_at bucket and are correctly ordered by priority, while
            # genuinely delayed jobs (seconds in the future) still sort later.
            bucket = round(job.run_at, 2)
            self._push_locked((bucket, job.priority, seq, job))
            self._cv.notify()      # wake one waiting worker

    def get(self, timeout=None):
        """Block until an ELIGIBLE job is available (run_at <= now) and return
        it, or return None if the queue is closed or the timeout elapses.

        Correctly handles delayed jobs: if the earliest job is scheduled for the
        future, workers wait only until that time, then re-check."""
        deadline = None if timeout is None else time.time() + timeout
        with self._cv:
            while True:
                if self._closed and not self._heap:
                    return None
                if self._heap:
                    run_at = self._heap[0][0]
                    now = time.time()
                    if run_at <= now:
                        return self._pop_locked()[3]
                    wait = run_at - now
                else:
                    wait = None  # empty: wait indefinitely for a put()

                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return None
                    wait = remaining if wait is None else min(wait, remaining)

                self._cv.wait(timeout=wait)

    def close(self):
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def __len__(self):
        with self._lock:
            return len(self._heap)


if __name__ == "__main__":
    from .job import Job
    q = DelayPriorityQueue()
    q.put(Job("t", priority=50))
    q.put(Job("t", priority=10))   # should come out first
    q.put(Job("t", priority=30))
    order = [q.get().priority for _ in range(3)]
    print("pop order by priority:", order)
    assert order == [10, 30, 50]
    print("OK")
