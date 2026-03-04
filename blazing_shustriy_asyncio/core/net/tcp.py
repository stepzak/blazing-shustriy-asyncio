from ...rust_core import PyTcpListener, PyTcpStream

class TcpListener:
    __slots__ = ("_impl",)
    
    def __init__(self):
        self._impl = PyTcpListener()
    
    async def bind(self, addr: str):
        await self._impl.bind(addr)
        return self
    
    async def accept(self):
        stream_impl = await self._impl.accept()
        print(stream_impl)
        stream = TcpStream()
        stream._impl = stream_impl
        return stream

class TcpStream:
    __slots__ = ("_impl",)

    def __init__(self):
        self._impl = None

    @classmethod
    async def connect(cls, addr: str):
        stream = cls()
        stream._impl = await PyTcpStream.connect(addr)
        return stream

    def is_connected(self) -> bool:
        return self._impl.is_connected() if self._impl else False
    
    def peer_address(self) -> str:
        if not self._impl:
            raise ConnectionError("Not connected")
        return self._impl.peer_addr()
    
    async def write(self, data: bytes):
        if not self._impl:
            raise ConnectionError("Not connected")
        return await self._impl.write(data)
    
    async def read(self, size: int):
        if not self._impl:
            raise ConnectionError("Not connected")
        return await self._impl.read(size)