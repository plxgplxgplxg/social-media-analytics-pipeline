import nest_asyncio
import asyncio
from telethon import TelegramClient

nest_asyncio.apply()

API_ID = 38781163
API_HASH = '28e58a98f52c80182b6def05378d7784'
CHANNEL_NAME = 'VnExpressOfficial'

async def fetch_telegram_data():
    
    async with TelegramClient('hust_session', API_ID, API_HASH) as client:
        print(f"--- crawl data from Telegram: {CHANNEL_NAME} ---")
        
       
        async for message in client.iter_messages(CHANNEL_NAME, limit=10):
            if message.text:
                print(f" get ID: {message.id}")
                print(f" Content: {message.text[:100]}...")
                print("-" * 30)

if __name__ == "__main__":
    asyncio.run(fetch_telegram_data())