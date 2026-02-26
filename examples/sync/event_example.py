from blazing_shustriy_asyncio import EventLoop, sleep, gather, Event

async def waiter(event, id):
    print(f"Waiter {id} waiting...")
    await event.wait()
    print(f"Waiter {id} woke up!")

async def setter(event):
    await sleep(0.5)
    print("Setting event...")
    event.set()

async def main():
    event = Event()
    
    tasks = [waiter(event, i) for i in range(3)]
    tasks.append(setter(event))
    
    await gather(*tasks)
    
    assert event.is_set() == True
    
    event.clear()
    assert event.is_set() == False
    
    print("Event test passed!")

if __name__ == "__main__" or True:
    loop = EventLoop()
    loop.run_until_complete(main())