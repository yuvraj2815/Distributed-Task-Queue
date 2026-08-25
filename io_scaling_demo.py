"""
io_scaling_demo.py
==================
Shows WHERE a thread-based task queue actually helps: I/O-bound work. For trivial
CPU tasks the GIL and journal I/O dominate, so more threads don't speed things up
(the benchmark shows this honestly). But real background jobs — HTTP calls, DB
queries, file/network I/O — spend most of their time WAITING, and threads overlap
that waiting. This demo simulates I/O-bound jobs with a short sleep and shows
throughput scaling with worker count.

Run:  python examples/io_scaling_demo.py
"""

import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from taskq.scheduler import TaskQueue

N = 200
IO_LATENCY = 0.05   # 50 ms of simulated I/O wait per job


def run(num_workers):
    d = tempfile.mkdtemp()
    tq = TaskQueue(num_workers=num_workers,
                   journal_path=os.path.join(d, "j.log"),
                   dlq_path=os.path.join(d, "dlq.log"))
    tq.register("io", lambda: time.sleep(IO_LATENCY))
    for _ in range(N):
        tq.submit("io")
    t0 = time.perf_counter()
    tq.start()
    tq.wait_until_idle(timeout=120)
    while tq.stats["succeeded"] < N and time.perf_counter() - t0 < 120:
        time.sleep(0.01)
    dt = time.perf_counter() - t0
    tq.shutdown(wait=False)
    return dt


def main():
    print(f"I/O-bound scaling demo: {N} jobs, {IO_LATENCY*1000:.0f} ms I/O each")
    print(f"Serial lower bound: {N * IO_LATENCY:.1f}s\n")
    baseline = None
    for w in [1, 2, 4, 8, 16]:
        dt = run(w)
        if baseline is None:
            baseline = dt
        print(f"  {w:2d} workers : {dt:6.2f}s   speedup {baseline/dt:4.1f}x")
    print("\nThreads overlap I/O wait, so throughput scales with workers here —")
    print("the real use case for a thread-based background queue.")


if __name__ == "__main__":
    main()
