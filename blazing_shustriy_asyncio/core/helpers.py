import typing as t

class _AsyncSleep:
    __slots__ = ("duration",)

    def __init__(self, duration: float):
        self.duration = duration

    def __await__(self):
        return (yield self)


class _AsyncGather:
    __slots__ = ("coros",)

    def __init__(self, *coros):
        self.coros = coros

    def __await__(self):
        return (yield self)

async def sleep(duration: float):
    """
    Sleep for {duration} seconds
    
    :param duration: duration of sleep in seconds
    :type duration: float
    ```
    await sleep(1.0)
    ```
    """
    return await _AsyncSleep(duration)

async def gather(*coros: t.Awaitable[t.Any]) -> list:
    """
    Gather coroutines and return their results
    
    :param coros: coroutines to run
    :type coros: t.Awaitable[t.Any]
    """
    return await _AsyncGather(*coros)