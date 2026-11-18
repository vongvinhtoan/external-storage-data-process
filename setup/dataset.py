"""A DatasetFolder-shaped dataset backed by the simulated external storage.

Mirrors `torch.utils.data.DatasetFolder`:
  * it scans its samples up front (`indices`);
  * `__len__` and `__getitem__` walk them by position;
  * `__getitem__` performs the actual load (here: the storage read).

`transform=None`: the CPU step stays in the caller, which keeps the sync and
async pipelines on the same processing code.

`astream()` is the async counterpart of iterating the dataset, with read-ahead
like a DataLoader's prefetch: the next sample is already being fetched while the
consumer works on the current one.
"""

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Iterator

from setup.problem import Problem
from setup.storage import ExternalStorage

Sample = tuple[int, list[int]]  # (key, data)


class StorageDataset:
    def __init__(self, problem: Problem, storage: ExternalStorage) -> None:
        self.problem = problem
        self.storage = storage
        self.indices = problem.indices

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, index: int) -> Sample:
        i = self.indices[index]
        return i, self.storage.read_at(i)

    def __iter__(self) -> Iterator[Sample]:
        for index in range(len(self)):
            yield self[index]

    async def astream(self, prefetch: int = 2) -> AsyncIterator[Sample]:
        """Yield samples in order while up to `prefetch` reads run ahead."""
        if prefetch < 1:
            raise ValueError("prefetch must be at least 1")

        queue: deque[asyncio.Task[Sample]] = deque()
        for index in range(len(self)):
            queue.append(asyncio.create_task(self._aget(index)))
            if len(queue) >= prefetch:
                yield await queue.popleft()
        while queue:
            yield await queue.popleft()

    async def _aget(self, index: int) -> Sample:
        i = self.indices[index]
        return i, await self.storage.read_at_async(i)
