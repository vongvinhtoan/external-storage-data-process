"""Synchronous solution: iterate the dataset, blocking on every load and write.

Same shape as a plain DataLoader with `num_workers=0`: `ds[i]` loads one sample
at a time and nothing else happens until it returns.
"""

import time

from setup.dataset import StorageDataset
from setup.problem import Problem


def run(problem: Problem) -> float:
    """Process every sample, returning the elapsed wall time in seconds."""
    storage = problem.storage()
    dataset = StorageDataset(problem, storage)
    started = time.perf_counter()

    for i, data in dataset:
        processed = problem.process(data)
        storage.write_at(i, processed)

    return time.perf_counter() - started
