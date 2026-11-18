# external-storage-data-process

A small benchmark that contrasts a **synchronous** pipeline with an **asynchronous streaming**
pipeline for data that lives in slow external storage.

The async side mirrors the shape of `torch.utils.data.DatasetFolder` + `DataLoader`: an indexed
dataset whose `__getitem__` performs the actual load, consumed either directly or through a
prefetching async stream. Nothing here touches a real disk — storage latency is simulated, which
makes the three scenarios (I/O-bound, CPU-bound and a heavy-tailed "skewed" one) reproducible and
fast to reason about.

## What it shows

- **I/O-bound** work: async streaming with read-ahead wins, because loads overlap with processing
  and writes.
- **CPU-bound** work: async streaming gains nothing, because synchronous processing blocks the event
  loop — the same reason `asyncio` cannot parallelize CPU work.
- **Overlapping stages** (`pipeline_sol`): running processing in a worker thread so it no longer
  blocks writes lifts throughput to `max(read, process, write)` instead of the sum — worth a lot
  when two or more stages are comparably expensive, worth little when one stage dominates.
- **Skewed latency** (`skewed`): real storage is not uniform — most calls are fast, a few are much
  slower. The scenario draws from a log-normal distribution so the mean exceeds the median and p99 is
  ~5× the median.
- Prefetch depth behaves like `DataLoader(prefetch_factor=...)`: throughput rises until the pipeline
  is limited by its slowest stage, then stops improving.

## Layout

```
setup/
  storage.py   ExternalStorage: read_at / write_at (blocking) and
               read_at_async / write_at_async (awaitable), with simulated latencies
  problem.py   Problem object (read/process/write costs + indices + skew), plus
               the IO_BOUND, CPU_BOUND and SKEWED scenarios
  dataset.py   StorageDataset: DatasetFolder-shaped __len__ / __getitem__ / __iter__,
               and astream(prefetch=...) for ordered read-ahead
solution/
  sync_sol.py     run(problem)                -> elapsed seconds
  async_sol.py    run(problem, prefetch=2)    -> elapsed seconds
  pipeline_sol.py three-stage read/process/write pipeline, bounded queues
run.py           runs every solution against every scenario
```

## Usage

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv run python run.py                        # 100 indices, prefetch 2
uv run python run.py --indices 20           # shorter run
uv run python run.py --prefetch 8           # deeper read-ahead
uv run python run.py --indices 500 --prefetch 4
uv run python run.py --scenario skewed      # one scenario only (repeatable)
```

### Output

```
$ uv run python run.py
prefetch=2
scenario   items solution    total   per item   vs sync
-------------------------------------------------------
io-bound     100 sync        4.22s     42.2ms     1.00x
io-bound     100 async       3.24s     32.4ms     1.30x
io-bound     100 pipeline    3.10s     31.0ms     1.36x

cpu-bound    100 sync        5.66s     56.6ms     1.00x
cpu-bound    100 async       5.55s     55.5ms     1.02x
cpu-bound    100 pipeline    5.48s     54.8ms     1.03x

skewed       100 sync        5.16s     51.6ms     1.00x
skewed       100 async       4.10s     41.0ms     1.26x
skewed       100 pipeline    3.93s     39.3ms     1.31x
```

Latencies are drawn from per-stage seeded RNGs, and `run.py` builds a fresh `Problem` for every
solution, so all solutions face an identical sequence of read/process/write costs. The comparison is
therefore purely structural; only wall-clock jitter (a few percent) moves between runs.

## The model

Each item goes through three stages:

| Stage   | Sync solution              | Async solution                 | Range (io-bound) | Range (cpu-bound) |
| ------- | -------------------------- | ------------------------------ | ---------------- | ----------------- |
| read    | `storage.read_at(i)`       | `await storage.read_at_async(i)` | 1–10 ms        | 0.1–1 ms          |
| process | `problem.process(data)`    | `problem.process(data)` (same) | 0.2–2 ms         | 20–80 ms          |
| write   | `storage.write_at(i, ...)` | `await storage.write_at_async(...)` | 5–50 ms     | 0.5–2 ms          |

The `skewed` scenario keeps the io-bound ranges but samples them from a log-normal distribution
(`skew=0.75`). The costs then look like:

| stage   | median | mean  | p95    | p99     |
| ------- | ------ | ----- | ------ | ------- |
| read    | 5.5 ms | 7.3 ms| 18.8 ms| 31.0 ms |
| process | 1.1 ms | 1.4 ms| 3.7 ms | 6.0 ms  |
| write   | 27.6 ms| 36.4 ms| 94.2 ms| 156.9 ms|

The tail is the point: ~1% of writes take more than 5× the median, so a run is a mix of mostly cheap
operations and occasional stalls. Where those stalls land decides whether they can be hidden — here
they are in the writes, which are serial, so they cannot (see below).

The sync solution is a plain `for` over the dataset. The async solution consumes
`dataset.astream(prefetch=n)`, which keeps up to `n` loads in flight while the consumer processes
and writes — the queue is FIFO, so sample order is preserved and memory stays bounded by `n`.

Throughput therefore approaches `max(read, process, write)` rather than their sum:

| prefetch | sync  | async | pipeline |
| -------- | ----- | ----- | -------- |
| 1        | 42.0 ms | 39.6 ms | 31.0 ms |
| 2        | 42.2 ms | 32.4 ms | 31.0 ms |
| 4        | 41.9 ms | 32.4 ms | 31.1 ms |
| 8        | 41.7 ms | 32.5 ms | 30.9 ms |

Read-ahead pays once (prefetch 1 → 2); beyond that the write stage sets the floor (~27.5 ms + a
little overhead), so a deeper queue has nothing left to hide.

### Overlapping process and write

`pipeline_sol` splits the work into three coroutines connected by bounded queues:

```
reader (await read) -> processor (asyncio.to_thread(process)) -> writer (await write)
```

Processing runs in a thread, so the writer drains earlier samples while the next one is being
transformed. With `io-bound` the write stage dominates and the gain over `async_sol` is small; the
effect is clear when the stages are comparable (30 samples, 15–25 ms per stage):

| solution | per item |
| -------- | -------- |
| sync     | 72.2 ms  |
| async    | 44.7 ms  |
| pipeline | 24.7 ms  |

That is `sum(stages)` → `max(stages)`.

### A heavy tail does not bend the floor

`skewed` moves the tail into the writes (p99 ≈ 157 ms). Deeper prefetch cannot absorb a slow write,
because the writer is a single ordered stage: every write must finish before the next starts, so the
run is bounded below by the sum of write latencies (~36.4 ms/item here).

| prefetch | sync  | async | pipeline |
| -------- | ----- | ----- | -------- |
| 1        | 50.3 ms | 47.8 ms | 37.9 ms |
| 2        | 50.6 ms | 39.9 ms | 37.9 ms |
| 4        | 50.3 ms | 39.7 ms | 37.9 ms |
| 8        | 50.6 ms | 39.7 ms | 37.9 ms |
| 16       | 50.5 ms | 39.8 ms | 37.9 ms |

The pipeline sits on the write floor from the first sample and stays there (it overlaps reads and
processing, not writes); `async_sol` needs prefetch ≥ 2 to reach its own plateau. Going from 2 to 16
changes nothing — the tail is on the critical path, not in the queue. Absorbing it would require
concurrent writes, which trades ordering guarantees for throughput.

## Caveats

- `time.sleep` overshoots its target slightly more than `asyncio.sleep`, so the sync solution pays a
  small constant penalty per storage call (~0.3 ms at 1 ms, ~3 ms at 30 ms). That, plus per-stage
  queue and thread overhead, is why measured ratios sit a little below the ideal stage bound.
- Wall-clock numbers still wobble by a few percent between runs even though the latency draws are
  seeded — scheduling, not sampling, is the remaining source of variation.
- CPU work is simulated with `time.sleep`, which releases the GIL; the async solution deliberately
  keeps processing synchronous, so no hidden parallelism is introduced.
- `--indices N` has no upper bound, but runtime grows linearly (~42 ms/item sync io-bound,
  ~57 ms/item sync cpu-bound; the async variants are ~25–35% faster on io-bound).
