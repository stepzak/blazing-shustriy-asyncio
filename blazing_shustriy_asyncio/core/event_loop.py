from .. import rust_core as _rc
import typing as t


class EventLoop:
    """
    High-perfomance Rust Event Loop(Blazing shustriy).
    
    Example:
    ```
        loop = EventLoop()
        loop.run_until_complete(main())
    ```
    """
    
    def __init__(self) -> None:
        
        self._impl: _rc.PyEventLoop = _rc.PyEventLoop()

    def create_task(self, coro: t.Awaitable[t.Any]) -> _rc.PyFuture:
        """Creates task out ouf coroutine"""
        return self._impl.create_task(coro)
    
    def spawn(self, coro: t.Awaitable[t.Any]) -> None:
        return self._impl.spawn(coro)

    def run_forever(self) -> None:
        """Runs event loop while it has tasks"""
        self._impl.run_forever()

    def run_until_complete(self, coro: t.Awaitable[t.Any]) -> t.Any:
        """Runs event loop until coro is done"""
        return self._impl.run_until_complete(coro)
