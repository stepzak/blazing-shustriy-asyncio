import os
from blazing_shustriy_asyncio.http.app import BlazingApp

app = BlazingApp()

@app.get("/test")
async def test(request):
    return "OK"


app.run(n_workers=os.cpu_count())