# main.py
import asyncio
import logging

from app.config import LOG_LEVEL
from app.scraper import HNScraper
from app.storage import NewsStorage


def setup_logging() -> None:
    """Настраивает формат и уровень логирования."""
    logging.basicConfig(
        level=LOG_LEVEL,  # ← ВАЖНО: должен быть INFO или DEBUG
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),  # вывод в консоль
            # При желании можно добавить logging.FileHandler("hn_scraper.log")
        ],
    )


async def main() -> None:
    setup_logging()  # <-- Добавляем настройку логов
    storage = NewsStorage("news_data.json")
    print(f"📁 Storage path: {storage.file_path.resolve()}")

    async with HNScraper(storage) as scraper:
        await scraper.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Scraper stopped by user.")