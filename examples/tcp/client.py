import time
from blazing_shustriy_asyncio import TcpStream, EventLoop, gather, sleep


async def client(i: int):
    stream = await TcpStream.connect("127.0.0.1:8080")
    await sleep(0.5)
    await stream.write(b"Hello")
    response = await stream.read(1024)
    print(f"Response: {response}")
    print(f"Client {i} finished")

async def main(n: int):
    now = time.perf_counter()
    await gather(*[client(i) for i in range(n)])
    print(time.perf_counter() - now)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main(10))