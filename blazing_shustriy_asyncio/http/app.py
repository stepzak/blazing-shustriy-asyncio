from .. import rust_core as _rc
import multiprocessing as mp

class BlazingApp:
    __slots__ = ("_router")

    def __init__(self):
        self._router = _rc.http.PyRouter()

    def route(self, method: str, path: str):
        def decorator(func):
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
        pass #TODO

    def run(self, host: str = "127.0.0.1", port: int = 8080, n_workers = 1):
        print(f"Server starting on {host:port}")
        if n_workers == 1:
            self._run_worker(host, port)
        else:
            processes = []
            for i in range(n_workers):
                p = mp.Process(target = self._run_worker, args = (host, int))
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
        