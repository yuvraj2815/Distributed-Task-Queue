"""
benchmark.py
============
Measures job throughput and how it scales with worker count, plus the overhead of
durable journaling. Runs locally against a temp directory.

Run:  python benchmarks/benchmark.py
"""

import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from taskq.scheduler import TaskQueue

N = 20_000


def run(num_workers, cpu_work=False):
    d = tempfile.mkdtemp()
    tq = TaskQueue(num_workers=num_workers,
                   journal_path=os.path.join(d, "j.log"),
                   dlq_path=os.path.join(d, "dlq.log"))

    def noop():
        if cpu_work:
            s = 0
            for i in range(200):
                s += i * i
            return s
        return 1

    tq.register("noop", noop)

    # submit
    t0 = time.perf_counter()
    for _ in range(N):
        tq.submit("noop")
    submit_dt = time.perf_counter() - t0

    # drain
    t0 = time.perf_counter()
    tq.start()
    tq.wait_until_idle(timeout=120)
    # give any stragglers a moment
    while tq.stats["succeeded"] < N and time.perf_counter() - t0 < 120:
        time.sleep(0.02)
    drain_dt = time.perf_counter() - t0
    tq.shutdown(wait=False)

    return {
        "workers": num_workers,
        "submit_tps": N / submit_dt,
        "process_tps": tq.stats["succeeded"] / drain_dt,
        "succeeded": tq.stats["succeeded"],
    }


def main():
    print(f"Task queue benchmark  ({N:,} jobs, durable journaling on)\n")
    print(f"Submit throughput (journaled, fsync per job):")
    r = run(4)
    print(f"  {r['submit_tps']:,.0f} jobs/s enqueued\n")

    print("Processing throughput vs. worker count:")
    for w in [1, 2, 4, 8]:
        r = run(w)
        print(f"  {w:2d} workers : {r['process_tps']:>10,.0f} jobs/s  "
              f"({r['succeeded']:,} completed)")

    out = os.path.join(os.path.dirname(__file__), "results.md")
    with open(out, "w") as f:
        f.write("# Benchmark results\n\n")
        f.write(f"{N:,} trivial jobs, durable journaling (fsync per state change).\n\n")
        f.write("| Workers | Processing throughput (jobs/s) |\n")
        f.write("|---------|-------------------------------|\n")
        for w in [1, 2, 4, 8]:
            r = run(w)
            f.write(f"| {w} | {r['process_tps']:,.0f} |\n")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
