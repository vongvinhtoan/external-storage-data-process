import asyncio
import time
from typing import Literal, Mapping, Protocol, TypeAlias, TypedDict

StorageOperationType: TypeAlias = Literal["read"] | Literal["write"]


def _data_at(i: int) -> list[int]:
    """Stand-in for the values stored under key `i`."""
    return [i, i + 1, i + 2, i + 3]


class DelayType(Protocol):
    def __call__(self, i: int) -> float: ...


class StorageOperation(TypedDict):
    operation: StorageOperationType
    delay: DelayType


class StorageOperationNotPermitted(Exception):
    def __init__(self, storage: "ExternalStorage", operation: StorageOperationType):
        message = f"Operation {operation} is not permitted on {storage}"
        super().__init__(message)


class ExternalStorage:
    name: str | None
    ops: Mapping[StorageOperationType, DelayType]

    def __init__(self, *operations: StorageOperation, name: str | None = None) -> None:
        super().__init__()

        self.name = name

        self.ops = {}
        for op in operations:
            self.ops[op["operation"]] = op["delay"]

    def __repr__(self) -> str:
        name = self.name
        ops = sorted(self.ops.keys())
        return f"{self.__class__.__name__}({name=}, {ops=})"

    def _wait_time(self, i: int, op_name: StorageOperationType) -> float:
        if op_name not in self.ops:
            raise StorageOperationNotPermitted(self, op_name)

        delay_func = self.ops[op_name]
        return delay_func(i)

    def _operate_at(self, i: int, op_name: StorageOperationType) -> None:
        # Blocking: simulates the latency of the external storage.
        time.sleep(self._wait_time(i, op_name))

    async def _operate_at_async(self, i: int, op_name: StorageOperationType) -> None:
        # Non-blocking: yields to the event loop while the storage is busy.
        await asyncio.sleep(self._wait_time(i, op_name))

    def read_at(self, i: int) -> list[int]:
        self._operate_at(i, "read")
        return _data_at(i)

    def write_at(self, i: int, data: list[int]) -> None:
        self._operate_at(i, "write")

    async def read_at_async(self, i: int) -> list[int]:
        await self._operate_at_async(i, "read")
        return _data_at(i)

    async def write_at_async(self, i: int, data: list[int]) -> None:
        await self._operate_at_async(i, "write")


if __name__ == "__main__":
    storage_child = ExternalStorage(
        {"operation": "write", "delay": lambda i: 10},
        {"operation": "read", "delay": lambda i: i**2},
        name="meo",
    )
    print(storage_child)
