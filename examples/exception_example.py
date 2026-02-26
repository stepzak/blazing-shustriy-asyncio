from blazing_shustriy_asyncio import EventLoop, sleep

async def bad_task():
    await sleep(0.0)
    raise Exception("Bruh")

async def main():
    try:
        await bad_task()
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())