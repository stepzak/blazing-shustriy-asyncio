from .. import rust_core as _rc
import typing as t

class Semaphore:
    """
    Async semaphore

    can be used via context manager:\n

    ```
    async with semaphore:
        ...
    ```
    """
    __slots__ = ("_impl",)

    def __init__(self, max_busy: int) -> None:
        self._impl: _rc.sync.PyLock = _rc.sync.PySemaphore(max_busy)

    async def acquire(self) -> None:
        """Acquires semaphore"""
        await self._impl.acquire()

    def release(self) -> None:
        """Releases semaphore"""
        self._impl.release()

    def locked(self) -> bool:
        return self._impl.locked()

    async def __aenter__(self) -> "Semaphore":
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: t.Any,
    ) -> None:
        self.release()