import asyncio
from typing import Any, Callable, Optional
from .. import rust_core


class BlazingFuture:
    """Blazing fast Future implementation compatible with asyncio.Future"""
    
    def __init__(self, loop=None):
        self._loop = loop
        self._rust_future = rust_core.PyFuture()
        self._callbacks: dict[Callable[['BlazingFuture'], None]] = []
        self._done = False
        self._result = None
        self._exception = None
        self._task_id = None
    
    def set_task_id(self, task_id: int) -> None:
        self._task_id = task_id
        self._rust_future.set_task_id(task_id)
    
    def set_result(self, result: Any) -> None:
        if self._done:
            raise asyncio.InvalidStateError("Future already done")
        self._result = result
        self._done = True
        self._rust_future.set_result(result)
        self._schedule_callbacks()
    
    def set_exception(self, exception: Exception) -> None:
        if self._done:
            raise asyncio.InvalidStateError("Future already done")
        self._exception = exception
        self._done = True
        self._rust_future.set_exception(exception)
        self._schedule_callbacks()
    
    def add_done_callback(self, callback: Callable[['BlazingFuture'], None]) -> None:
        if self._done:
            callback(self)
        else:
            self._callbacks.append(callback)
    
    def remove_done_callback(self, callback: Callable[['BlazingFuture'], None]) -> int:
        count = 0
        while callback in self._callbacks:
            self._callbacks.remove(callback)
            count += 1
        return count
    
    def _schedule_callbacks(self) -> None:
        for cb in self._callbacks:
            if self._loop:
                self._loop.call_soon(cb, self)
            else:
                cb(self)
        self._callbacks.clear()
    
    def result(self) -> Any:
        if not self._done:
            raise asyncio.InvalidStateError("Future not done")
        if self._exception:
            raise self._exception
        return self._result
    
    def exception(self) -> Optional[Exception]:
        if not self._done:
            raise asyncio.InvalidStateError("Future not done")
        return self._exception
    
    def done(self) -> bool:
        return self._done
    
    def cancelled(self) -> bool:
        return self._exception is not None and isinstance(self._exception, asyncio.CancelledError)
    
    def cancel(self) -> bool:
        if self._done:
            return False
        self._rust_future.cancel()
        return True
    
    def get_loop(self):
        return self._loop
    
    def __await__(self):
        if not self._done:
            yield self
        return self.result()
    
    __iter__ = __await__