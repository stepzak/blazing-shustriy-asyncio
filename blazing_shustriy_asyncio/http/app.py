from blazing_shustriy_asyncio.core.helpers import sleep
from .. import rust_core as _rc
import multiprocessing as mp
import typing as t

class BlazingApp:
    __slots__ = ("_router", "_host", "_port")

    def __init__(self):
        self._router = _rc.http.PyRouter()
        self._host = "127.0.0.1"
        self._port = 8080

    def route(self, method: str, path: str):
        def decorator(func: t.Awaitable):
            self._router.add_route(method, path, func)
            return func
        return decorator

    def get(self, path: str):
        return self.route("GET", path)

    def post(self, path: str):
        return self.route("POST", path)

    def patch(self, path: str):
        return self.route("PATCH", path)

    def put(self, path: str):
        return self.route("PUT", path)

    def delete(self, path: str):
        return self.route("DELETE", path)

    def _run_worker(self, host: str, port: int):
        self._router.freeze()

        loop = _rc.PyEventLoop()
        listener = _rc.PyTcpListener()
        print("Worker started")
        async def start_server():
            await listener.bind(f"{host}:{port}", router=self._router)
            print("Bound")
            while True:
                await listener.accept()
        try:
            loop.run_until_complete(start_server())
        except KeyboardInterrupt:
            loop.stop()

    def run(self, host: str = "127.0.0.1", port: int = 8080, n_workers: int = 1):
        print(f"Server starting on {host}:{port}")
        self._host = host
        self._port = port
        
        if n_workers == 1:
            self._run_worker(host, port)
        else:
            try:
                mp.set_start_method('fork', force=True)
            except RuntimeError:
                pass

            processes = []
            for i in range(n_workers):
                p = mp.Process(target=self._run_worker, args=(host, port))
                p.start()
                processes.append(p)

            try:
                for p in processes:
                    p.join()
            except KeyboardInterrupt:
                print("Shutting down...")
                for p in processes:
                    p.terminate()
                    p.join()
                print("Server stopped.")
