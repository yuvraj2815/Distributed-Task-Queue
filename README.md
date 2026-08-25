# Distributed Task Queue

A durable, fault-tolerant background job queue built from scratch — a thread-pool
scheduler with priority ordering, delayed/scheduled execution, automatic retries
with exponential backoff, a dead-letter queue, crash recovery via a write-ahead
journal, and graceful shutdown. No external broker (Redis/RabbitMQ) and no
job-queue library: the priority heap, the durability layer, and the worker pool
are all implemented here.

This demonstrates the concepts backend/SDE interviews probe: concurrency
primitives, a hand-written data structure, at-least-once delivery semantics,
durability, and reliability under failure.

## Features

- **Priority scheduling** — lower priority value runs first; FIFO within a
  priority (stable ordering).
- **Delayed / scheduled jobs** — submit with a `delay`; workers block efficiently
  until the job's time arrives (no busy-waiting).
- **Retries with exponential backoff** — failed jobs are re-enqueued with growing
  delay until `max_retries` is exhausted.
- **Dead-letter queue** — jobs that exhaust retries are preserved for inspection,
  never silently dropped.
- **Crash recovery** — every state change is journaled (fsync); on restart,
  outstanding jobs are replayed. At-least-once execution.
- **Graceful shutdown** — stops accepting work, drains in-flight jobs, joins
  workers cleanly.
- **Metrics** — submitted / succeeded / retried / failed / dead counts.

## Quickstart

```python
from taskq.scheduler import TaskQueue

tq = TaskQueue(num_workers=4)

@tq.task("send_email")
def send_email(to):
    ...

tq.start()
tq.submit("send_email", "vip@example.com", priority=1)     # jumps the queue
tq.submit("send_email", "later@example.com", delay=60)     # runs in 60s
tq.submit("charge_card", 500, max_retries=3)               # retried on failure
tq.shutdown()                                              # drains gracefully
```

```bash
git clone https://github.com/yuvraj2815/distributed-task-queue.git
cd distributed-task-queue
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python examples/quickstart.py       # end-to-end demo
python examples/io_scaling_demo.py  # shows thread scaling on I/O-bound work
python benchmarks/benchmark.py      # throughput
pytest -q                           # 11 tests incl. crash recovery
```

## Architecture

```
   submit()                         ┌──────────────┐
      │        append + fsync        │   Journal    │  durability / recovery
      ▼ ───────────────────────────► │ (write-ahead)│
 ┌─────────────────────┐            └──────────────┘
 │  DelayPriorityQueue │  binary min-heap keyed by (run_at, priority, seq)
 │  (thread-safe, CV)  │  — delayed jobs wait; workers block, no busy-wait
 └──────────┬──────────┘
            │ get()
     ┌──────┴───────┐  N worker threads
     ▼      ▼       ▼
  [worker][worker][worker]  run task → success | retry (backoff) | dead-letter
            │
            ▼
   ┌──────────────┐
   │ Dead-letter  │  exhausted-retry jobs, preserved & auditable
   └──────────────┘
```

## Reliability model

- **At-least-once execution.** A job is journaled before and after running. A job
  interrupted by a crash is replayed and retried on restart. (This implies tasks
  should be idempotent — the standard contract for at-least-once queues.)
- **Retries with backoff.** Attempt *n* waits `base · 2^(n-1)` seconds before
  re-running, so transient failures recover without hammering a failing
  dependency.
- **Torn-write safety.** The journal is append-only; a partially written final
  line from a crash fails to parse and is skipped on replay, so earlier records
  are never corrupted.

## Performance & honest scaling

Throughput of ~3,000 trivial jobs/s with durable journaling. For **CPU-bound**
trivial tasks, throughput is roughly flat across worker counts — Python's GIL and
the fsync-per-write journal serialize the work, and more threads don't help. This
is expected and worth understanding, not hiding.

Where a thread-based queue **does** scale is **I/O-bound** work — the real use
case for background jobs (HTTP calls, DB queries, file/network I/O). Threads
overlap the waiting:

| Workers | Time for 200 × 50 ms I/O jobs | Speedup |
|---------|-------------------------------|---------|
| 1  | 10.2 s | 1.0× |
| 4  | 2.6 s  | 4.0× |
| 8  | 1.3 s  | 7.9× |
| 16 | 0.7 s  | 15.3× |

Near-linear, because I/O wait is overlapped. Run `examples/io_scaling_demo.py`.

## Project structure

```
distributed-task-queue/
├── taskq/
│   ├── job.py             # Job model + states + backoff
│   ├── priority_queue.py  # from-scratch thread-safe delay-aware min-heap
│   ├── persistence.py     # write-ahead journal + dead-letter queue
│   └── scheduler.py       # worker pool, retries, recovery, shutdown
├── examples/
│   ├── quickstart.py
│   └── io_scaling_demo.py
├── benchmarks/
│   └── benchmark.py
├── tests/
│   └── test_taskq.py
├── requirements.txt
└── README.md
```

## Design notes

- **Hand-written binary heap** (not `heapq`) keyed by `(run_at, priority, seq)` —
  one structure handles priority, delay, and FIFO tie-breaking together.
- **Condition variable, not polling** — idle workers block until a job is ready or
  a delayed job's time arrives, so there's no busy-wait CPU burn.
- **Tasks registered by name** — keeps jobs serializable for the journal, so they
  can be reconstructed on recovery.

## Possible extensions

Multi-process workers (bypass the GIL for CPU work), Redis-backed queue for true
distribution across machines, job dependencies / DAGs, cron-style recurring jobs,
result backend.

## License
MIT.
