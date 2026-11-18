"""Asynchronous solution: stream the dataset with read-ahead.

`dataset.astream(prefetch=...)` keeps the next loads in flight while the current
sample is processed and written, which is the async analogue of a DataLoader's
prefetch. Processing itself stays ordinary synchronous code: a plain loop over
the data returned by the read.
"""

import asyncio
import time

from setup.dataset import StorageDataset
from setup.problem import Problem

DEFAULT_PREFETCH = 2  # current sample + one read running ahead


async def run_async(problem: Problem, prefetch: int = DEFAULT_PREFETCH) -> float:
    """Process every sample, returning the elapsed wall time in seconds."""
    storage = problem.storage()
    dataset = StorageDataset(problem, storage)
    started = time.perf_counter()

    async for i, data in dataset.astream(prefetch=prefetch):
        # CPU-bound: a plain loop over the read data, deliberately not async.
        processed = problem.process(data)

        await storage.write_at_async(i, processed)

    return time.perf_counter() - started


def run(problem: Problem, prefetch: int = DEFAULT_PREFETCH) -> float:
    """Blocking entry point matching `solution.sync_sol.run`."""
    return asyncio.run(run_async(problem, prefetch=prefetch))
