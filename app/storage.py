# storage.py
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from queue import Queue
from threading import Thread
from time import sleep
from typing import Dict, List

logger = logging.getLogger(__name__)


class NewsStorage(Thread):
    def __init__(self, file_path: str = "hn_stories.json"):
        super().__init__()
        self.file_path = Path(file_path)
        self.queue = Queue()
        self.current_data = None

        # Проверка: не является ли путь директорией
        if self.file_path.exists() and self.file_path.is_dir():
            raise FileExistsError(f"Cannot create storage: '{self.file_path}' is a directory, not a file.")

        # Создаём родительскую папку
        if not self.file_path.parent.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)

        self.daemon = True
        self.start()

        logger.info(f"✅ NewsStorage initialized: {self.file_path.resolve()}")

    def run(self):
        while True:
            if not self.queue.empty():
                data_to_save = self.queue.get()
                self._save_story(story_id=data_to_save[0], data=data_to_save[1])
            else:
                sleep(0.5)

            self.current_data = self.load()


    def load(self) -> Dict:
        try:
            if not self.file_path.exists():
                logger.debug(f"🆕 File not found: {self.file_path}. Creating empty storage.")
                return {"stories": {}}

            if self.file_path.stat().st_size == 0:
                logger.warning(f"🗑️ File is empty: {self.file_path}. Resetting.")
                return {"stories": {}}

            with open(self.file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if not content.strip():
                    return {"stories": {}}
                data = json.loads(content)

            if not isinstance(data, dict):
                logger.warning("Loaded data is not a dict.")
                return {"stories": {}}
            if "stories" not in data:
                data["stories"] = {}

            logger.debug(f"📥 Loaded {len(data['stories'])} stories.")
            return data

        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {self.file_path}: {e}")
            return {"stories": {}}
        except Exception as e:
            logger.error(f"💥 Failed to load {self.file_path}: {e}", exc_info=True)
            return {"stories": {}}

    def _write_file(self, data: Dict):
        try:
            logger.debug(f"📝 About to write to {self.file_path}")
            logger.debug(f"📁 File path type: {type(self.file_path)}")
            logger.debug(f"📄 Full path: {self.file_path.resolve()}")

            # Проверка родительской директории
            if not self.file_path.parent.exists():
                logger.warning(f"⚠️ Parent dir does not exist: {self.file_path.parent}")
                self.file_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"✅ Created parent dir: {self.file_path.parent}")

            with open(self.file_path, "w", encoding="utf-8") as f:
                logger.debug("⚡ Starting to write content...")
                f.write(json.dumps(data, ensure_ascii=False, indent=2))
                logger.debug("✅ Content written successfully")

            logger.info(f"🎉 Successfully saved to {self.file_path}")
        except Exception as e:
            logger.error(f"💥 Failed to write file: {type(e).__name__}: {e}", exc_info=True)
            raise

    async def has_story(self, story_id: str) -> bool:
        data = self.current_data
        return story_id in data["stories"]

    async def save_story(self, story_id: str, data: dict):
        self.queue.put([story_id, data])

    def _save_story(self, story_id: str, data: dict):
        data = data.copy()
        data["updated_at"] = datetime.now().isoformat()

        logger.debug(f"🔒 Lock acquired for saving story {story_id}")
        data_store = self.load()
        data_store["stories"][story_id] = data
        self._write_file(data_store)
        logger.debug(f"💾 Story {story_id} saved successfully")


    async def get_all_story_ids(self) -> List[str]:
        data = self.current_data
        if not data:
            return []
        return list(data["stories"].keys())