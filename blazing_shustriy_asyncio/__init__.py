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

async def gather(*coros):
    return await AsyncGather(*coros)


EventLoop = rust_core.PyEventLoop
Lock = rust_core.sync.PyLock

__all__ = ["EventLoop", "Lock", "sleep", "gather"]