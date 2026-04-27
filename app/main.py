# main.py
import asyncio
from app.scraper import HNScraper
from app.storage import NewsStorage

async def main():
    storage = NewsStorage("news_data.json")
    print(f"📁 Storage path: {storage.file_path.resolve()}")

    async with HNScraper(storage) as scraper:
        await scraper.run()  # ← Бесконечный цикл

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Scraper stopped by user.")