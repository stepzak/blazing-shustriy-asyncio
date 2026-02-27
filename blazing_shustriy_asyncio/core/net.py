from .. import rust_core as _rc

class TcpListener:
    __slots__ = ("_impl",)
    
    def __init__(self):
        self._impl = _rc.PyTcpListener()

    def bind(self, addr: str) -> None:
        self._impl.bind(addr)
    
    async def accept(self):
        print("Accepting...")
        client_impl = await self._impl.accept()
        print(client_impl)
        wrapper = TcpStream()
        wrapper._impl = client_impl
        return wrapper

class TcpStream:
    __slots__ = ("_impl",)
    
    def __init__(self):
        self._impl = _rc.PyTcpStream()

    async def connect(self, addr: str):
        new_impl = await self._impl.connect(addr)
        if new_impl is not None:
            self._impl = new_impl
        return self

    async def read(self, size: int):
        return await self._impl.read(size)
    
    async def write(self, data):
        if isinstance(data, bytearray):
            data = bytes(data)
        return await self._impl.write(data)