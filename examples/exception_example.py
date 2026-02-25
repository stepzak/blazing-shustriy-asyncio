from blazing_shustriy_asyncio import EventLoop, AsyncSleep

def bad_task():
    yield AsyncSleep(0.0)
    raise Exception("Bruh")

def main():
    try:
        yield bad_task()
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    loop = EventLoop()
    loop.run_until_complete(main())