from blazing_shustriy_asyncio import TcpListener, TcpStream
from blazing_shustriy_asyncio.core.event_loop import EventLoop

async def main(loop: EventLoop):
    # Сервер
    listener = await TcpListener().bind("127.0.0.1:8080")
    
    async def handle_client(stream):
        while True:
            data = await stream.read(1024)
            if not data:  # None при закрытии
                break
            print(f"Received: {data}")
            await stream.write(b"ECHO: " + data)
    
    # Принимаем клиентов
    while True:
        stream, addr = await listener.accept()
        loop.spawn(handle_client(stream=stream))
    
    # Клиент
    stream = await TcpStream.connect("127.0.0.1:8080")
    await stream.write(b"Hello")
    response = await stream.read(1024)
    print(f"Response: {response}")
    await stream.close()

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main(loop))