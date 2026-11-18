# external-storage-data-process

A small benchmark that contrasts a **synchronous** pipeline with an **asynchronous streaming**
pipeline for data that lives in slow external storage.

The async side mirrors the shape of `torch.utils.data.DatasetFolder` + `DataLoader`: an indexed
dataset whose `__getitem__` performs the actual load, consumed either directly or through a
prefetching async stream. Nothing here touches a real disk — storage latency is simulated, which
makes the three scenarios (I/O-bound, CPU-bound and balanced) reproducible and fast to reason
about.

## What it shows

- **I/O-bound** work: async streaming with read-ahead wins, because loads overlap with processing
  and writes.
- **CPU-bound** work: async streaming gains nothing, because synchronous processing blocks the event
  loop — the same reason `asyncio` cannot parallelize CPU work.
- **Overlapping stages** (`pipeline_sol`): running processing in a worker thread so it no longer
  blocks writes lifts throughput to `max(read, process, write)` instead of the sum — worth ~3x over
  sync in the `balanced` scenario, and almost nothing when a single stage dominates.
- Prefetch depth behaves like `DataLoader(prefetch_factor=...)`: throughput rises until the pipeline
  is limited by its slowest stage, then stops improving.

## Layout

```
setup/
  storage.py   ExternalStorage: read_at / write_at (blocking) and
               read_at_async / write_at_async (awaitable), with simulated latencies
  problem.py   Problem object (read/process/write costs + indices), plus the
               IO_BOUND, CPU_BOUND and BALANCED scenarios
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
uv run python run.py --scenario balanced     # one scenario (repeatable)
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
cpu-bound    100 async       5.59s     55.9ms     1.01x
cpu-bound    100 pipeline    5.46s     54.6ms     1.04x

balanced     100 sync        7.13s     71.3ms     1.00x
balanced     100 async       4.60s     46.0ms     1.55x
balanced     100 pipeline    2.40s     24.0ms     2.97x
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
transformed. With `io-bound` the write stage dominates and the gain over `async_sol` is small; with
`cpu-bound` processing dominates, so the same is true. The regime that shows the pipeline is
`balanced`, where read, process and write all cost ~20 ms:

| solution | per item |
| -------- | -------- |
| sync     | 71.3 ms  |
| async    | 46.0 ms  |
| pipeline | 24.0 ms  |

That is `sum(stages)` → `max(stages)`: about 3x over sync and 2x over async. The gain shrinks as
one stage starts to dominate — which is exactly what `io-bound` and `cpu-bound` demonstrate.

## Caveats

- `time.sleep` overshoots its target slightly more than `asyncio.sleep`, so the sync solution pays a
  small constant penalty per storage call (~0.3 ms at 1 ms, ~3 ms at 30 ms). That, plus per-stage
  queue and thread overhead, is why measured ratios sit a little below the ideal stage bound.
- CPU work is simulated with `time.sleep`, which releases the GIL; the async solution deliberately
  keeps processing synchronous, so no hidden parallelism is introduced.
- Wall-clock numbers still wobble by a few percent between runs even though the latency draws are
  seeded — scheduling, not sampling, is the remaining source of variation.
- `--indices N` has no upper bound, but runtime grows linearly (~42 ms/item sync io-bound,
  ~57 ms/item sync cpu-bound; the async variants are ~25–35% faster on io-bound).
