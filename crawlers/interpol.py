"""
Crawler for Interpol Red Notices.
Source: https://ws-public.interpol.int/notices/v1/red
Format: JSON REST API (paginated)
~7,000 entities
"""

import time
from crawlers.base import BaseCrawler, logger


class InterpolCrawler(BaseCrawler):
    SOURCE_NAME = "interpol"
    SOURCE_URL = "https://ws-public.interpol.int/notices/v1/red"
    
    def fetch(self) -> bytes:
        """Fetch is handled in parse() due to pagination."""
        return b""
    
    def parse(self, raw_data: bytes) -> list[dict]:
        """Paginate through all Red Notices."""
        entities = []
        page = 1
        per_page = 160  # Max allowed by API
        
        while True:
            try:
                resp = self.session.get(
                    self.SOURCE_URL,
                    params={'page': page, 'resultPerPage': per_page},
                    timeout=30
                )
                
                if resp.status_code == 429:
                    logger.warning("[interpol] Rate limited, waiting 10s...")
                    time.sleep(10)
                    continue
                
                resp.raise_for_status()
                data = resp.json()
                
                notices = data.get('_embedded', {}).get('notices', [])
                if not notices:
                    break
                
                for notice in notices:
                    entity = self._parse_notice(notice)
                    if entity:
                        entities.append(entity)
                
                # Check if more pages
                total = data.get('total', 0)
                if page * per_page >= total:
                    break
                
                page += 1
                time.sleep(0.5)  # Be polite
                
            except Exception as e:
                logger.error(f"[interpol] Error on page {page}: {e}")
                break
        
        return entities
    
    def _parse_notice(self, notice: dict) -> dict:
        entity_id = notice.get('entity_id', '')
        if not entity_id:
            # Extract from _links
            self_link = notice.get('_links', {}).get('self', {}).get('href', '')
            entity_id = self_link.split('/')[-1] if self_link else str(hash(str(notice)))
        
        forename = notice.get('forename', '') or ''
        name = notice.get('name', '') or ''
        
        full_name = f"{forename} {name}".strip()
        if not full_name:
            return None
        
        # Nationalities
        nationalities = notice.get('nationalities', []) or []
        
        # Date of birth
        birth_dates = []
        dob = notice.get('date_of_birth')
        if dob:
            birth_dates.append(dob)
        
        # Place of birth
        birth_places = []
        pob = notice.get('place_of_birth')
        if pob:
            birth_places.append(pob)
        
        # Arrest warrants contain charge info
        reasons_parts = []
        for warrant in notice.get('arrest_warrants', []):
            charge = warrant.get('charge', '')
            country = warrant.get('issuing_country_id', '')
            if charge:
                reasons_parts.append(f"{charge} ({country})" if country else charge)
        
        # Thumbnail
        thumbnail = ''
        thumbnails = notice.get('_links', {}).get('thumbnail', {})
        if isinstance(thumbnails, dict):
            thumbnail = thumbnails.get('href', '')
        
        return {
            'source_id': str(entity_id),
            'entity_type': 'person',
            'full_name': full_name,
            'first_name': forename,
            'last_name': name,
            'aliases': [],
            'identifiers': [],
            'nationalities': nationalities,
            'addresses': [],
            'birth_dates': birth_dates,
            'birth_places': birth_places,
            'programs': ['Interpol Red Notice'],
            'reasons': '; '.join(reasons_parts),
            'source_url': f"https://www.interpol.int/en/How-we-work/Notices/Red-Notices/View-Red-Notices#{entity_id}",
        }
    
    # Uses BaseCrawler.run() — fetch() returns b"", parse() handles pagination
