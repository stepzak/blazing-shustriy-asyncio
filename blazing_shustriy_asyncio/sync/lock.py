from .. import rust_core as _rc
import typing as t

class Lock:
    """
    Async lock

    can be used via context manager:\n

    ```
    async with lock:
        ...
    ```
    """
    __slots__ = ("_impl",)
    
    def __init__(self) -> None:
        self._impl: _rc.sync.PyLock = _rc.sync.PyLock()

    async def acquire(self) -> None:
        """Acquires lock"""
        await self._impl.acquire()

    def release(self) -> None:
        """Releases Lock"""
        self._impl.release()

    def locked(self) -> bool:
        return self._impl.locked()

    async def __aenter__(self) -> "Lock":
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: t.Any,
    ) -> None:
        self.release()