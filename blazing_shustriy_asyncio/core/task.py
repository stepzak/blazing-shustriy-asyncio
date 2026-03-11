import asyncio

from blazing_shustriy_asyncio.core.future import BlazingFuture


class BlazingTask(asyncio.Task):
    """Blazing fast Task implementation compatible with asyncio.Task"""
    
    def __init__(self, coro, *, loop=None, name=None):
        super().__init__(coro, loop=loop, name=name)
        self._task_id = None
        self._blazing_future = None
    
    def set_task_id(self, task_id: int) -> None:
        self._task_id = task_id
    
    def set_future(self, future: BlazingFuture) -> None:
        self._blazing_future = future
    
    def cancel(self, msg=None) -> bool:
        if self._blazing_future:
            return self._blazing_future.cancel()
        return super().cancel(msg)
    
    def get_loop(self):
        return self._loop
    
    @classmethod
    def current_task(cls, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        task_id = loop.current_task_id()
        if task_id is not None:
            return loop.get_task(task_id)
        return None