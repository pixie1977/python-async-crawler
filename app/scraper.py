# scraper.py
import asyncio
from typing import List, Dict

import aiohttp
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import logging

from config import BASE_URL, UPDATE_INTERVAL, TOP_STORIES_COUNT
from storage import NewsStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HNScraper:
    def __init__(self, storage: NewsStorage):
        self.storage = storage
        self.session = None

    async def __aenter__(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; HN-Scraper/1.0)"
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch(self, url: str) -> str:
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"HTTP {response.status} for {url}")
                    return ""
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""

    def extract_links_from_text(self, text: str) -> List[str]:
        # Улучшенный регекс для извлечения URL
        urls = re.findall(
            r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?',
            text
        )
        return list(set(u.rstrip(".") for u in urls))  # Убираем дубли и точки на конце

    async def parse_story_page(self, item_id: str) -> Dict:
        url = f"{BASE_URL}/item?id={item_id}"
        html = await self.fetch(url)
        if not html:
            logger.warning(f"No HTML content for story {item_id}")
            return {"links": []}

        soup = BeautifulSoup(html, "html.parser")
        links = []

        # Ищем все комментарии (класс commtext или других модификаторов)
        comment_divs = soup.find_all("div", class_="commtext")
        for div in comment_divs:
            text = div.get_text(separator=" ", strip=True)
            extracted = self.extract_links_from_text(text)
            links.extend(extracted)

        unique_links = list(set(links))
        logger.info(f"Extracted {len(unique_links)} unique links from comments of story {item_id}")
        return {"links": unique_links}

    async def parse_top_stories(self) -> List[Dict]:
        """
        Парсит топ-новости с главной страницы Hacker News.
        Учитывает, что каждая новость состоит из двух строк: <tr class="athing"> и следующий <tr> с .subtext.
        """
        html = await self.fetch(BASE_URL)
        if not html:
            logger.error("Failed to fetch main page.")
            return []

        soup = BeautifulSoup(html, "html.parser")
        stories = []
        rows = soup.find_all("tr")

        count = 0
        for row in rows:
            if count >= TOP_STORIES_COUNT:
                break

            # Находим строку с классом "athing" — это заголовок новости
            if not row.has_attr("class") or "athing" not in row["class"]:
                continue

            entry = row
            title_span = entry.find("span", class_="titleline")
            if not title_span or not title_span.find("a"):
                continue

            link_tag = title_span.find("a")
            story = {
                "title": link_tag.get_text(strip=True),
                "url": link_tag.get("href"),
                "id": str(entry.get("id")),
            }

            # Обработка относительных ссылок
            if story["url"].startswith("/"):
                story["url"] = urljoin(BASE_URL, story["url"])

            # Поиск метаданных в следующих строках
            next_row = entry.find_next_sibling("tr")
            score = "0"
            user = "unknown"

            while next_row and not next_row.find("span", class_="subtext"):
                # Пропускаем пустые строки между athing и subtext
                next_row = next_row.find_next_sibling("tr")
                if not next_row:
                    break

            if next_row:
                subtext = next_row.find("span", class_="subtext")
                if subtext:
                    score_elem = subtext.find("span", class_="score")
                    score = score_elem.get_text(strip=True).split()[0] if score_elem else "0"
                    user_elem = subtext.find("a", class_="hnuser")
                    user = user_elem.get_text(strip=True) if user_elem else "unknown"

            story["score"] = score
            story["user"] = user
            stories.append(story)
            count += 1

        logger.info(f"Parsed {len(stories)} stories from front page.")
        return stories

    async def process_story(self, story: Dict):
        story_id = story["id"]

        if await self.storage.has_story(story_id):
            logger.debug(f"Story {story_id} already processed, skipping.")
            return

        logger.info(f"Processing new story: {story['title']} (ID: {story_id}, Score: {story['score']})")

        comment_data = await self.parse_story_page(story_id)

        full_data = {
            "title": story["title"],
            "url": story["url"],
            "score": story["score"],
            "user": story["user"],
            "comments_links": comment_data["links"]
        }

        # === ДОБАВЬТЕ ЭТО: try-except + логи ===
        try:
            logger.debug(f"💾 Attempting to save story {story_id} to storage...")
            await self.storage.save_story(story_id, full_data)
            logger.info(f"✅ Successfully saved story {story_id} with {len(comment_data['links'])} links.")
        except Exception as e:
            logger.exception(f"💥 FAILED to save story {story_id}: {e}")

    async def run_cycle(self):
        logger.info("Starting fetch cycle...")
        stories = await self.parse_top_stories()

        if not stories:
            logger.warning("No stories were parsed. Check network or HTML structure.")
            return

        logger.info(f"Fetched {len(stories)} stories. Processing...")

        # Асинхронная обработка всех новостей
        tasks = [self.process_story(story) for story in stories]
        await asyncio.gather(*tasks)

        logger.info("Fetch cycle completed.")

    async def run(self):
        logger.info(f"HN Scraper started. Polling every {UPDATE_INTERVAL} seconds.")
        while True:
            try:
                await self.run_cycle()
            except Exception as e:
                logger.error(f"Unexpected error during cycle: {e}", exc_info=True)
            await asyncio.sleep(UPDATE_INTERVAL)