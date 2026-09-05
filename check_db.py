import asyncio, asyncpg, os
from dotenv import load_dotenv
load_dotenv()

async def run():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    
    # Check distinct state values in scrape_locations
    states = await conn.fetch("SELECT state, COUNT(*) as cnt FROM scrape_locations GROUP BY state ORDER BY cnt DESC")
    print("=== States in scrape_locations ===")
    for r in states:
        print(f"  state='{r['state']}' | count={r['cnt']}")
    
    # Check a sample row
    sample = await conn.fetchrow("SELECT id, area, city, district, state, status, latitude, longitude FROM scrape_locations LIMIT 1")
    print("\n=== Sample row ===")
    print(dict(sample))
    
    # Check what SCRAPER_STATE env var is set to
    print("\n=== SCRAPER_STATE env var ===")
    print(os.getenv('SCRAPER_STATE', 'NOT SET'))
    
    await conn.close()

asyncio.run(run())
