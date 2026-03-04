from blazing_shustriy_asyncio import TcpListener, TcpStream
from blazing_shustriy_asyncio.core.event_loop import EventLoop

async def main(loop: EventLoop):
    listener = await TcpListener().bind("127.0.0.1:8080")
    print("Bound")
    async def handle_client(stream):
        print("Handling client...")
        while True:
            data = await stream.read(1024)
            if not data:
                break
            print(f"Received: {data}")
            await stream.write(b"ECHO: " + data)
    
    while True:
        stream = await listener.accept()
        print(stream._impl)
        loop.spawn(handle_client(stream=stream))

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main(loop))