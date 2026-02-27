import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from blazing_shustriy_asyncio import EventLoop
# Убедись, что путь к оберткам верный. Если они в __init__.py, то импорт такой:
from blazing_shustriy_asyncio import TcpListener, TcpStream

async def handle_client(client_id: int, stream):
    """
    Обработчик клиента.
    В этом тесте мы ничего не делаем с сокетом, просто закрываем его.
    """
    print(f"🟢 [Handler {client_id}] Client connected. Closing immediately.")
    # В реальном сервере здесь были бы read/write
    # Здесь мы просто удаляем объект stream, что должно закрыть сокет в Rust (Drop)
    del stream
    print(f"🔴 [Handler {client_id}] Client closed.")

async def server_main():
    host = "127.0.0.1"
    port = 9999
    
    listener = TcpListener()
    
    print(f"⚙️  Binding to {host}:{port}...")
    listener.bind(f"{host}:{port}")
    print(f"✅ Server bound. Listening for connections...")
    print(f"💡 Tip: Connect using 'telnet {host} {port}' or 'nc {host} {port}' in another terminal.")
    print("Press Ctrl+C to stop.\n")

    client_count = 0
    
    while True:
        # Ждем подключения. Эта строка тестирует твою очередь AcceptOp.
        # Если очередь работает, несколько задач могут ждать здесь одновременно (хотя здесь одна задача в цикле).
        # Но если ты запустишь этот скрипт и подключишь 5 клиентов быстро, 
        # цикл будет принимать их по одному.
        try:
            client_stream = await listener.accept()
            client_count += 1
            print(f"📥 [Server] Accepted connection #{client_count}")
            
            # Запускаем обработку в фоне, чтобы сразу вернуться на accept()
            # Это создаст конкуренцию: пока один клиент обрабатывается, сервер ждет следующего.
            loop.spawn(handle_client(client_count, client_stream))
            
        except Exception as e:
            print(f"❌ Error accepting connection: {e}")
            break

if __name__ == "__main__":
    loop = EventLoop()
    try:
        loop.run_until_complete(server_main())
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user.")
    except Exception as e:
        print(f"\n💥 Critical Error: {e}")
        import traceback
        traceback.print_exc()