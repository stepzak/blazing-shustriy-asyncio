import time
from blazing_shustriy_asyncio import EventLoop, AsyncGather, AsyncSleep

def worker(name: str, delay: str):
    yield AsyncSleep(delay)
    return f"Task {name} finished"

def main():
    start = time.perf_counter()
    results = yield AsyncGather(
        worker("task 1", 0.5),
        worker("task 2", 0.8)
    )

    print(results)
    print(f"Total time: {time.perf_counter() - start}s")

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())