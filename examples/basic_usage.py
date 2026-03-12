import asyncio
import time
from blazing_shustriy_asyncio import BlazingEventLoop as EventLoop, sleep, gather

async def worker(name: str, delay: str):
    return f"Task {name} finished"

async def main():
    start = time.perf_counter()
    results = await worker("1", 1)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())