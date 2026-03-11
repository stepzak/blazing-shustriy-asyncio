import asyncio
from typing import Any, Callable, Coroutine, Optional, Union
from .. import rust_core
import threading
import contextvars
import time
from .future import BlazingFuture
from .task import BlazingTask


class BlazingHandle:
    def __init__(self, handle_id: int, loop: 'BlazingEventLoop'):
        self._handle_id = handle_id
        self._loop = loop
        self._cancelled = False
    
    def cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            self._loop._rust_loop.cancel_handle(self._handle_id)
    
    def cancelled(self) -> bool:
        return self._cancelled


class BlazingEventLoop(asyncio.AbstractEventLoop):
    def __init__(self):
        self._rust_loop = rust_core.PyEventLoop()
        self._thread_id = threading.get_ident()
        self._closed = False
        self._running = False
        self._tasks: dict[int, asyncio.Task] = {}
        self._handles: dict[int, BlazingHandle] = {}
        self._default_executor = None
        self._exception_handler = None
        self._task_factory = None
        self._time = time.monotonic
        self._debug = False
    
    def get_debug(self) -> bool:
        return self._debug
    
    def spawn(self, coro):
        self._rust_loop.spawn(coro)

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    def _check_closed(self):
        if self._closed:
            raise RuntimeError('Event loop is closed')
    
    def _check_thread(self):
        if threading.get_ident() != self._thread_id:
            raise RuntimeError('Event loop is not running in the same thread')
    
    def run_forever(self):
        self._check_closed()
        self._check_thread()
        self._running = True
        asyncio._set_running_loop(self)
        
        try:
            self._rust_loop.run_forever()
        finally:
            self._running = False
    
    def run_until_complete(self, future: Union[asyncio.Future, Coroutine]):
        self._check_closed()
        self._check_thread()
        
        if not asyncio.isfuture(future):
            future = self.create_task(future)
        
        self._running = True
        
        asyncio._set_running_loop(self)
        self._rust_loop.run_until_complete(future)
        self._running = False
        print(future)
        return future.result()
    
    def stop(self):
        self._rust_loop.stop()
    
    def is_running(self) -> bool:
        return self._running
    
    def is_closed(self) -> bool:
        return self._closed
    
    def close(self):
        if self._running:
            raise RuntimeError('Cannot close a running event loop')
        self._closed = True
    
    def time(self) -> float:
        return self._rust_loop.time()
    
    def create_task(self, coro: Coroutine, *, name: Optional[str] = None, context: Optional[contextvars.Context] = None) -> asyncio.Task:
        self._check_closed()
        
        task = BlazingTask(coro, loop=self, name=name)
        task_id = self._rust_loop.spawn(coro)
        task.set_task_id(task_id)
        self._tasks[task_id] = task
        
        return task
    
    async def shutdown_asyncgens(self) -> None:
        pass
    
    async def shutdown_default_executor(self) -> None:
        if self._default_executor:
            self._default_executor.shutdown(wait=True)
    
    def create_future(self) -> asyncio.Future:
        self._check_closed()
        return BlazingFuture(loop=self)
    
    def call_soon(self, callback: Callable, *args: Any, context: Optional[contextvars.Context] = None) -> BlazingHandle:
        self._check_closed()
        
        if context is None:
            context = contextvars.copy_context()
        
        handle_id = self._rust_loop.call_soon(callback, args, context)
        handle = BlazingHandle(handle_id, self)
        self._handles[handle_id] = handle
        return handle
    
    def call_later(self, delay: float, callback: Callable, *args: Any, context: Optional[contextvars.Context] = None) -> BlazingHandle:
        self._check_closed()
        return self.call_at(self.time() + delay, callback, *args, context=context)
    
    def call_at(self, when: float, callback: Callable, *args: Any, context: Optional[contextvars.Context] = None) -> BlazingHandle:
        self._check_closed()
        
        if context is None:
            context = contextvars.copy_context()
        
        handle_id = self._rust_loop.call_later(when - self.time(), callback, args, context)
        handle = BlazingHandle(handle_id, self)
        self._handles[handle_id] = handle
        return handle
    
    def cancel_handle(self, handle_id: int) -> bool:
        try:
            self._rust_loop.cancel_handle(handle_id)
            return True
        except:
            return False
    
    def current_task(self) -> Optional[asyncio.Task]:
        task_id = self._rust_loop.current_task()
        if task_id is not None:
            return self._tasks.get(task_id)
        return None
    
    def get_task(self, task_id: int) -> Optional[asyncio.Task]:
        return self._tasks.get(task_id)
    
    def cancel_task(self, task_id: int) -> bool:
        return self._rust_loop.cancel(task_id)
    
    def call_exception_handler(self, context: dict):
        if self._exception_handler:
            try:
                self._exception_handler(self, context)
            except Exception as e:
                print(f'Exception in exception handler: {e}')
        else:
            message = context.get('message')
            exception = context.get('exception')
            if message:
                print(f'Exception in callback: {message}')
            elif exception:
                print(f'Exception in callback: {exception}')
    
    def get_exception_handler(self):
        return self._exception_handler
    
    def set_exception_handler(self, handler):
        self._exception_handler = handler
    
    def get_default_exception_handler(self):
        return None
    
    def get_task_factory(self):
        return self._task_factory
    
    def set_task_factory(self, factory):
        self._task_factory = factory
    
    async def create_connection(self, protocol_factory, host=None, port=None, **kwargs):
        raise NotImplementedError
    
    async def create_server(self, protocol_factory, host=None, port=None, **kwargs):
        raise NotImplementedError
    
    async def sock_recv(self, sock, n):
        raise NotImplementedError
    
    async def sock_sendall(self, sock, data):
        raise NotImplementedError
    
    async def sock_accept(self, sock):
        raise NotImplementedError
    
    async def sock_connect(self, sock, address):
        raise NotImplementedError
    
    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        raise NotImplementedError
    
    async def getnameinfo(self, sockaddr, flags=0):
        raise NotImplementedError
    
    async def run_in_executor(self, executor, func, *args):
        if executor is None:
            executor = self._default_executor
        if executor is None:
            import concurrent.futures
            executor = concurrent.futures.ThreadPoolExecutor()
        
        future = self.create_future()
        
        def _run():
            try:
                result = func(*args)
                self.call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                self.call_soon_threadsafe(future.set_exception, e)
        
        executor.submit(_run)
        return await future
    
    def set_default_executor(self, executor):
        self._default_executor = executor
    
    def call_soon_threadsafe(self, callback, *args, context=None):
        handle = self.call_soon(callback, *args, context=context)
        self._rust_loop.wakeup()
        return handle


class BlazingEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    def _loop_factory(self):
        return BlazingEventLoop()
    
    def get_event_loop(self):
        try:
            return super().get_event_loop()
        except RuntimeError:
            loop = self.new_event_loop()
            self.set_event_loop(loop)
            return loop
    
    def new_event_loop(self):
        return self._loop_factory()