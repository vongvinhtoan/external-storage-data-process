# external-storage-data-process

A small benchmark that contrasts a **synchronous** pipeline with an **asynchronous streaming**
pipeline for data that lives in slow external storage.

The async side mirrors the shape of `torch.utils.data.DatasetFolder` + `DataLoader`: an indexed
dataset whose `__getitem__` performs the actual load, consumed either directly or through a
prefetching async stream. Nothing here touches a real disk — storage latency is simulated, which
makes the two scenarios (I/O-bound and CPU-bound) reproducible and fast to reason about.

## What it shows

- **I/O-bound** work: async streaming with read-ahead wins, because loads overlap with processing
  and writes.
- **CPU-bound** work: async streaming gains nothing, because synchronous processing blocks the event
  loop — the same reason `asyncio` cannot parallelize CPU work.
- **Overlapping stages** (`pipeline_sol`): running processing in a worker thread so it no longer
  blocks writes lifts throughput to `max(read, process, write)` instead of the sum — worth a lot
  when two or more stages are comparably expensive, worth little when one stage dominates.
- Prefetch depth behaves like `DataLoader(prefetch_factor=...)`: throughput rises until the pipeline
  is limited by its slowest stage, then stops improving.

## Layout

```
setup/
  storage.py   ExternalStorage: read_at / write_at (blocking) and
               read_at_async / write_at_async (awaitable), with simulated latencies
  problem.py   Problem object (read/process/write costs + indices), plus the
               IO_BOUND and CPU_BOUND scenarios
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
```

### Output

```
$ uv run python run.py
prefetch=2
scenario   items solution    total   per item   vs sync
-------------------------------------------------------
io-bound     100 sync        3.93s     39.3ms     1.00x
io-bound     100 async       2.91s     29.1ms     1.35x
io-bound     100 pipeline    2.91s     29.1ms     1.35x

cpu-bound    100 sync        5.25s     52.5ms     1.00x
cpu-bound    100 async       5.31s     53.1ms     0.99x
cpu-bound    100 pipeline    5.21s     52.1ms     1.01x
```

Numbers move between runs because every read, write and processing step draws a fresh random
latency from its configured range.

## The model

Each item goes through three stages:

| Stage   | Sync solution              | Async solution                 | Range (io-bound) | Range (cpu-bound) |
| ------- | -------------------------- | ------------------------------ | ---------------- | ----------------- |
| read    | `storage.read_at(i)`       | `await storage.read_at_async(i)` | 1–10 ms        | 0.1–1 ms          |
| process | `problem.process(data)`    | `problem.process(data)` (same) | 0.2–2 ms         | 20–80 ms          |
| write   | `storage.write_at(i, ...)` | `await storage.write_at_async(...)` | 5–50 ms     | 0.5–2 ms          |

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
