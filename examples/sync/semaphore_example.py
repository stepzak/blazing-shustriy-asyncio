from blazing_shustriy_asyncio import EventLoop, Semaphore, sleep, gather

sem = Semaphore(2)

async def worker(i: int):
    print(f"Task {i} started")
    async with sem:
        i*=2
        await sleep(2)
    print(f"Task {i//2} done")
    return i

async def main():
    workers = [worker(i) for i in range(10)]
    res = await gather(*workers)
    print(res)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())
