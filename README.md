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
io-bound     100 sync        3.77s     37.7ms     1.00x
io-bound     100 async       2.87s     28.7ms     1.31x
io-bound     100 pipeline    3.16s     31.6ms     1.19x

cpu-bound    100 sync        5.21s     52.1ms     1.00x
cpu-bound    100 async       5.51s     56.1ms     0.95x
cpu-bound    100 pipeline    5.51s     55.1ms     0.95x

skewed       100 sync        4.76s     47.6ms     1.00x
skewed       100 async       3.97s     39.7ms     1.20x
skewed       100 pipeline    4.50s     45.0ms     1.06x
```

Numbers move between runs because every read, write and processing step draws a fresh random
latency from its configured range; at 100 items the run-to-run spread is a few percent, and the
async variants land within roughly ±5% of each other unless one stage dominates.

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

The tail is the point: ~1% of writes take more than 5× the median, so a run is a mix of mostly
cheap operations and occasional stalls that async read-ahead can absorb.

The sync solution is a plain `for` over the dataset. The async solution consumes
`dataset.astream(prefetch=n)`, which keeps up to `n` loads in flight while the consumer processes
and writes — the queue is FIFO, so sample order is preserved and memory stays bounded by `n`.

Throughput therefore approaches `max(read, process, write)` rather than their sum:

| prefetch | io-bound per item |
| -------- | ----------------- |
| 1        | 38.4 ms (no read-ahead, same as sync) |
| 2        | 32.6 ms |
| 4        | 29.8 ms (asymptote ≈ 28.6 ms) |

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

## Caveats

- `time.sleep` overshoots its target slightly more than `asyncio.sleep`, so the sync solution pays a
  small constant penalty per storage call. That is why the io-bound ratio sits around 1.2–1.4×
  rather than exactly at the stage-bound limit.
- CPU work is simulated with `time.sleep`, which releases the GIL; the async solution deliberately
  keeps processing synchronous, so no hidden parallelism is introduced.
- `--indices N` has no upper bound, but runtime grows linearly (~37 ms/item io-bound, ~55 ms/item
  cpu-bound per solution).
