"""
Base crawler class for all sanctions sources.
Each source implements fetch() and parse() methods.
"""

import time
import logging
import requests
from abc import ABC, abstractmethod
from db.models import upsert_entities_batch, update_source_status

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """Base class for all sanctions list crawlers."""

    SOURCE_NAME: str = ""
    SOURCE_URL: str = ""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "SanctionsScreener/1.0 (leverandorscreening; kurs@forumoa.no)"
        })

    @abstractmethod
    def fetch(self) -> bytes:
        """Download raw data from source. Returns raw bytes."""
        pass

    @abstractmethod
    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse raw data into list of entity dicts."""
        pass

    def run(self):
        """Execute full sync: fetch, parse, batch-upsert to Supabase."""
        logger.info(f"[{self.SOURCE_NAME}] Starting sync...")
        start = time.time()

        try:
            raw = self.fetch()
            entities = self.parse(raw)

            # Tag all entities with source
            for entity in entities:
                entity['source'] = self.SOURCE_NAME

            # Batch upsert to Supabase
            count = upsert_entities_batch(entities)

            duration = time.time() - start
            update_source_status(None, self.SOURCE_NAME, count, duration)

            logger.info(f"[{self.SOURCE_NAME}] Synced {count} entities in {duration:.1f}s")
            return count

        except Exception as e:
            duration = time.time() - start
            logger.error(f"[{self.SOURCE_NAME}] Error: {e}")

            try:
                update_source_status(None, self.SOURCE_NAME, 0, duration, str(e))
            except:
                pass

            raise
