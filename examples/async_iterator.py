from blazing_shustriy_asyncio import sleep, EventLoop

async def iter(num: int):
    cur = 0
    while cur < num:
        await sleep(0.5)
        yield cur
        cur+=1

async def main():
    async for i in iter(5):
        print(i)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())