import asyncio, asyncpg, os
from dotenv import load_dotenv
load_dotenv()

async def run():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    result = await conn.execute("UPDATE scrape_locations SET state='Jharkhand' WHERE state='JHARKHAND'")
    print('Updated:', result)
    total = await conn.fetchval("SELECT COUNT(*) FROM scrape_locations WHERE state='Jharkhand' AND status='pending'")
    print('Pending with state=Jharkhand:', total)
    await conn.close()

asyncio.run(run())
