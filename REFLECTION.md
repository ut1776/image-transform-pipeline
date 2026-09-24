# Reflection

## Q1. What trade-offs does your scheduler make between throughput and fairness?

My scheduler admits jobs strictly in input order (FIFO). If the head job does not fit in the remaining memory budget, I wait for in-flight jobs to finish instead of skipping ahead to a smaller job that appears later. This gives up some throughput: a large head job can leave workers idle while smaller, ready jobs sit behind it. In exchange it is fair and starvation-free, since a large job can never be bypassed indefinitely by a stream of small ones, and admission order stays predictable and easy to reason about.

An oversized head job (bigger than the whole budget) is allowed to run, but only when nothing else is in flight, so it cannot be starved and cannot break the memory limit for others. I also read the input lazily, pulling the next job only when a worker slot is free, which bounds memory to at most `max_workers` admitted jobs plus one pending head.

If throughput mattered more than fairness, I could allow bounded look-ahead (skip the head for a small job a limited number of times, then force the head), which recovers most of the idle time while still guaranteeing the head eventually runs.

## Q2. How would you distribute a million-image batch while preserving idempotency and deterministic result ordering?

I would split the batch into fixed-size contiguous shards by input index (for example 1,000 images each) and assign each shard to a worker node through a queue. Every job keeps its global index, so the final ordering is defined by index and does not depend on which node finishes first. A coordinator (or a final merge step) sorts or writes results by index into an output store, giving deterministic ordering.

For idempotency, each shard has a stable ID and writes its output to a deterministic location keyed by shard ID and index (for example object-store paths), using atomic write-then-rename. If a worker crashes or a shard is retried or duplicated, it overwrites the same keys with identical content instead of creating duplicates. Transforms should be pure and deterministic. A manifest records which shards are complete, so a rerun only processes missing shards.

Failures stay per-image: a bad image produces an error record at its index rather than failing the shard. Each node runs the same bounded pipeline locally (worker limit and byte budget), and the coordinator limits how many shards are in flight per node to apply backpressure.

## Q3. What production memory would you measure beyond the input-byte budget used by this exercise?

The input-byte budget only counts the original `ImageJob.data`. It excludes transform outputs, decoded buffers, and executor overhead, and in production those are usually the larger share of memory. So I would track actual process memory (RSS) per worker and per process, including peak values, along with container or cgroup memory against its limit, OOM-kill counts, and memory growth over time to catch leaks and fragmentation.

I would also track decoded and intermediate buffers: the peak decoded size per image (a small compressed file can decode to hundreds of megabytes), the ratio of decoded size to input size, the peak footprint of the transform chain (the largest input and output alive at once, since each transform produces a new buffer), and the p99 and maximum single-image footprint, since one huge image can dominate.

For pipeline and executor overhead, I would measure queue depth, the number of pending futures, thread stacks and per-task objects, and the memory held by completed results. In my implementation, finished results are kept until the end, so output bytes accumulate without bound. In production I would stream or flush results as they complete.

At the runtime level, I would watch Python heap versus native allocations (native memory from the image library often does not show up in Python stats), garbage collection pressure, allocator fragmentation (RSS can stay high after buffers are freed), and swap usage or page-fault rates.

Finally, I would monitor admission-control signals: admission wait time, how often the head job blocks on the budget, how often an oversized head job runs alone and how long workers idle because of it, and budget utilization compared with real RSS. If RSS is much higher than the budget implies, the budget is mis-sized.

The action I would take from these metrics is to calibrate the budget against measured decoded size rather than raw input bytes, for example by charging each job an estimated cost (input bytes multiplied by a decode multiplier learned from real RSS) instead of its raw length.
