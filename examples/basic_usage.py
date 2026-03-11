import time
from blazing_shustriy_asyncio import BlazingEventLoop as EventLoop, sleep, gather

async def worker(name: str, delay: str):
    await sleep(delay)
    return f"Task {name} finished"

async def main():
    start = time.perf_counter()
    results = await gather(
        worker("task 1", 0.5),
        worker("task 2", 0.8)
    )

    print(results)
    print(f"Total time: {time.perf_counter() - start}s")

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())