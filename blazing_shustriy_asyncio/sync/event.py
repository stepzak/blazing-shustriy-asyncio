from .. import rust_core as _rc

class Event:
    __slots__ = ("_impl",)

    def __init__(self):
        self._impl = _rc.sync.PyEvent()

    async def wait(self):
        await self._impl.wait()

    def set(self):
        self._impl.set()

    def clear(self):
        self._impl.clear()

    @property
    def is_set(self):
        return self._impl.is_set()