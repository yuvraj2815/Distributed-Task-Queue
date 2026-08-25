"""
quickstart.py
=============
Minimal end-to-end example of using the task queue.

Run:  python examples/quickstart.py
"""

import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from taskq.scheduler import TaskQueue

d = tempfile.mkdtemp()
tq = TaskQueue(num_workers=4,
               journal_path=os.path.join(d, "journal.log"),
               dlq_path=os.path.join(d, "dead_letter.log"))


# Register tasks with the decorator.
@tq.task("send_email")
def send_email(to):
    print(f"  sent email to {to}")
    return True


@tq.task("charge_card")
def charge_card(amount):
    if amount < 0:
        raise ValueError("negative amount")   # will retry, then dead-letter
    print(f"  charged Rs {amount}")
    return amount


tq.start()

# High-priority job jumps ahead of normal ones.
tq.submit("send_email", "vip@example.com", priority=1)
for i in range(3):
    tq.submit("send_email", f"user{i}@example.com", priority=100)

# A delayed job (runs ~1s later).
tq.submit("send_email", "later@example.com", delay=1.0)

# A job that will fail and exhaust retries -> dead-letter queue.
tq.submit("charge_card", -50, max_retries=2)

# A normal successful job.
tq.submit("charge_card", 500)

tq.wait_until_idle()
time.sleep(1.5)     # let the delayed job and retries finish
tq.shutdown()

print("\nstats:", tq.stats)
print("dead-letter jobs:", len(tq.dlq))
for j in tq.dlq.all():
    print(f"  dead: {j.task_name}{j.args} — {j.last_error}")
