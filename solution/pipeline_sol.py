"""Async solution as a three-stage pipeline: read -> process -> write.

Unlike `async_sol`, processing runs in a worker thread (`asyncio.to_thread`) so
it does not block the event loop, and the write of one sample proceeds while the
next sample is being processed. Each stage has a single worker and FIFO queues,
so order is preserved and concurrency stays bounded by `prefetch`.
"""

import asyncio
import time

from setup.dataset import StorageDataset
from setup.problem import Problem

DEFAULT_PREFETCH = 2

Item = tuple[int, list[int]]


async def run_async(problem: Problem, prefetch: int = DEFAULT_PREFETCH) -> float:
    """Process every sample, returning the elapsed wall time in seconds."""
    storage = problem.storage()
    dataset = StorageDataset(problem, storage)

    read_queue: asyncio.Queue[Item | None] = asyncio.Queue(maxsize=prefetch)
    write_queue: asyncio.Queue[Item | None] = asyncio.Queue(maxsize=prefetch)

    async def reader() -> None:
        async for item in dataset.astream(prefetch=prefetch):
            await read_queue.put(item)
        await read_queue.put(None)  # sentinel: no more reads

    async def processor() -> None:
        while (item := await read_queue.get()) is not None:
            i, data = item
            # Off the event loop, so writes for earlier samples keep flowing.
            processed = await asyncio.to_thread(problem.process, data)
            await write_queue.put((i, processed))
        await write_queue.put(None)  # sentinel: no more writes

    async def writer() -> None:
        while (item := await write_queue.get()) is not None:
            i, processed = item
            await storage.write_at_async(i, processed)

    started = time.perf_counter()
    await asyncio.gather(reader(), processor(), writer())
    return time.perf_counter() - started


def run(problem: Problem, prefetch: int = DEFAULT_PREFETCH) -> float:
    """Blocking entry point matching the other solutions."""
    return asyncio.run(run_async(problem, prefetch=prefetch))
