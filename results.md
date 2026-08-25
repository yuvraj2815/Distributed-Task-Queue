# Benchmark results

20,000 trivial jobs, durable journaling (fsync per state change).

| Workers | Processing throughput (jobs/s) |
|---------|-------------------------------|
| 1 | 3,199 |
| 2 | 3,179 |
| 4 | 3,072 |
| 8 | 2,824 |
