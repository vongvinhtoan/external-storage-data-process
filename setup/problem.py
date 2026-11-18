"""The workloads the solutions are measured against.

A `Problem` bundles the three costs of the pipeline (read, process, write) plus
the data indices to work through. Two scenarios are provided:

* `IO_BOUND`  - storage latency dominates, the transform is cheap.
* `CPU_BOUND` - the transform dominates, storage latency is negligible.
"""

import random
import time
from collections.abc import Iterable
from dataclasses import dataclass, field, replace

from setup.storage import ExternalStorage

LatencyRange = tuple[float, float]  # (low, high) seconds


@dataclass(frozen=True)
class Problem:
    name: str
    read_latency: LatencyRange
    write_latency: LatencyRange
    process_latency: LatencyRange
    indices: tuple[int, ...] = field(default_factory=lambda: tuple(range(100)))

    def with_length(self, n: int) -> "Problem":
        """A copy of this problem with `n` indices."""
        return replace(self, indices=tuple(range(n)))

    def read_delay(self, i: int) -> float:
        return random.uniform(*self.read_latency)

    def write_delay(self, i: int) -> float:
        return random.uniform(*self.write_latency)

    def process_delay(self) -> float:
        return random.uniform(*self.process_latency)

    def process(self, data: Iterable[int]) -> list[int]:
        """CPU-only transform: a plain loop over the data that was read.

        No I/O happens here, so this is the same code for the sync and async
        pipelines; only the surrounding read/write calls differ.
        """
        processed: list[int] = []
        for value in data:
            processed.append(value * 2)

        # In a real pipeline the loop above *is* the cost. Simulate it so the
        # two scenarios can be compared on the same code path.
        time.sleep(self.process_delay())
        return processed

    def storage(self) -> ExternalStorage:
        """A fresh storage bound to this problem's read/write latencies."""
        return ExternalStorage(
            {"operation": "read", "delay": self.read_delay},
            {"operation": "write", "delay": self.write_delay},
            name=self.name,
        )


# Storage latency dominates: reads 1-10 ms, writes 5-50 ms, transform 0.2-2 ms.
IO_BOUND = Problem(
    name="io-bound",
    read_latency=(0.001, 0.010),
    write_latency=(0.005, 0.050),
    process_latency=(0.0002, 0.002),
)

# Transform dominates: 20-80 ms of compute against sub-millisecond I/O.
CPU_BOUND = Problem(
    name="cpu-bound",
    read_latency=(0.0001, 0.001),
    write_latency=(0.0005, 0.002),
    process_latency=(0.020, 0.080),
)

PROBLEMS = (IO_BOUND, CPU_BOUND)
