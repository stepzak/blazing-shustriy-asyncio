from blazing_shustriy_asyncio import Lock, EventLoop, gather, sleep

a = 0

lock = Lock()

async def worker(i: int):
    global a
    print(f"{i} started")
    async with lock:
        await sleep(0.5)
        if a == 0:
            a+=1
        print(f"{i} finished")

async def main():
    await gather(
        worker(1),
        worker(2)
    )

    print(a)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())