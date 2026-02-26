from . import rust_core

class AsyncSleep:
    __slots__ = ("duration",)

    def __init__(self, duration: float):
        self.duration = duration

    def __await__(self):
        return (yield self)


class AsyncGather:
    __slots__ = ("coros",)

    def __init__(self, *coros):
        self.coros = coros

    def __await__(self):
        return (yield self)

async def sleep(duration: float):
    return await AsyncSleep(duration)

class EventLoop:
    __slots__ = ("_native_loop")

    def __init__(self):
        self._native_loop = rust_core.PyEventLoop()
    
    def sleep(self, duration: float):
        return AsyncSleep(duration)
    
    def gather(self, *coros):
        return AsyncGather(*coros)
    
    def run_until_complete(self, main_coro):
        self._native_loop.run_until_complete(main_coro)