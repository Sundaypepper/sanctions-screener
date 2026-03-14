"""
Additional crawlers for: UK OFSI, World Bank, Canada, Australia DFAT,
Switzerland SECO, US BIS Denied Persons, US SAM Exclusions.
"""

import csv
import io
import json
import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler, logger


# ─────────────────────────────────────────────
# UK OFSI - Office of Financial Sanctions
# ─────────────────────────────────────────────
class UKOFSICrawler(BaseCrawler):
    SOURCE_NAME = "uk_ofsi"
    SOURCE_URL = "https://assets.publishing.service.gov.uk/media/65d89d2c6efa83001de2d3f1/UK_Sanctions_List.ods"
    # Alternative CSV endpoint:
    CSV_URL = "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv"
    
    def fetch(self) -> bytes:
        # Try CSV format first (easier to parse)
        resp = self.session.get(self.CSV_URL, timeout=120)
        if resp.status_code == 200:
            return resp.content
        # Fallback
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            text = raw_data.decode('utf-8-sig')
        except:
            text = raw_data.decode('latin-1')
        
        reader = csv.DictReader(io.StringIO(text))
        
        for row in reader:
            # Column names vary, try common patterns
            name6 = row.get('Name 6', '') or row.get('name6', '')
            name1 = row.get('Name 1', '') or row.get('name1', '')
            name2 = row.get('Name 2', '') or row.get('name2', '')
            name3 = row.get('Name 3', '') or row.get('name3', '')
            
            group_type = row.get('Group Type', '') or row.get('group_type', '')
            
            # Build name
            name_parts = [p for p in [name6, name1, name2, name3] if p and p.strip()]
            full_name = ' '.join(name_parts).strip()
            
            if not full_name:
                continue
            
            uid = row.get('Group ID', '') or row.get('group_id', '') or str(hash(full_name))
            
            entity_type = 'person' if 'individual' in group_type.lower() else 'company'
            
            # DOB
            birth_dates = []
            dob = row.get('DOB', '') or row.get('dob', '')
            if dob:
                birth_dates.append(dob)
            
            # Nationality
            nationalities = []
            nat = row.get('Nationality', '') or row.get('nationality', '')
            if nat:
                nationalities.append(nat)
            
            # Regime
            regime = row.get('Regime', '') or row.get('regime', '')
            
            entities.append({
                'source_id': str(uid),
                'entity_type': entity_type,
                'full_name': full_name,
                'first_name': name1,
                'last_name': name6 or name2,
                'aliases': [],
                'identifiers': [],
                'nationalities': nationalities,
                'addresses': [],
                'birth_dates': birth_dates,
                'birth_places': [],
                'programs': [regime] if regime else ['UK Sanctions'],
                'reasons': row.get('Other Information', ''),
                'legal_basis': 'UK Sanctions and Anti-Money Laundering Act 2018',
                'source_url': 'https://www.gov.uk/government/publications/the-uk-sanctions-list',
            })
        
        return entities


# ─────────────────────────────────────────────
# World Bank Debarment List
# ─────────────────────────────────────────────
class WorldBankCrawler(BaseCrawler):
    SOURCE_NAME = "worldbank"
    SOURCE_URL = "https://api.worldbank.org/v2/debarr?format=json&per_page=5000"
    # Alternative: scrape HTML from worldbank.org/debarred-firms
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            data = json.loads(raw_data)
        except:
            logger.warning("[worldbank] JSON parse failed, trying alternative format")
            return entities
        
        # World Bank API returns [metadata, records]
        records = data if isinstance(data, list) and len(data) > 1 else [data]
        if isinstance(records, list) and len(records) > 1:
            records = records[1] if isinstance(records[1], list) else records
        
        if not isinstance(records, list):
            return entities
        
        for record in records:
            if not isinstance(record, dict):
                continue
            
            firm_name = record.get('firm_name', '') or record.get('FirmName', '') or ''
            if not firm_name:
                continue
            
            # Determine if person or company
            entity_type = 'person' if any(t in firm_name.upper() for t in ['MR.', 'MR ', 'MS.', 'MS ', 'MRS.', 'DR.']) else 'company'
            
            from_date = record.get('from_date', '') or record.get('FromDate', '')
            to_date = record.get('to_date', '') or record.get('ToDate', '')
            grounds = record.get('grounds', '') or record.get('Grounds', '')
            country = record.get('country', '') or record.get('Country', '')
            
            entities.append({
                'source_id': firm_name[:100],  # No unique ID in API
                'entity_type': entity_type,
                'full_name': firm_name,
                'first_name': '',
                'last_name': firm_name,
                'aliases': [],
                'identifiers': [],
                'nationalities': [country] if country else [],
                'addresses': [country] if country else [],
                'birth_dates': [],
                'birth_places': [],
                'listed_date': from_date,
                'delisted_date': to_date,
                'programs': ['World Bank Debarment'],
                'reasons': grounds,
                'legal_basis': 'World Bank Sanctions Procedures',
                'source_url': 'https://www.worldbank.org/en/projects-operations/procurement/debarred-firms',
            })
        
        return entities


# ─────────────────────────────────────────────
# Canada - Consolidated Autonomous Sanctions
# ─────────────────────────────────────────────
class CanadaCrawler(BaseCrawler):
    SOURCE_NAME = "canada_sema"
    SOURCE_URL = "https://www.international.gc.ca/world-monde/assets/office-bureau/international_sanctions-sanctions_internationales.xml"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            root = ET.fromstring(raw_data)
        except ET.ParseError:
            logger.error("[canada] XML parse error")
            return entities
        
        for record in root.iter('record'):
            entity = self._parse_record(record)
            if entity:
                entities.append(entity)
        
        return entities
    
    def _parse_record(self, rec) -> dict:
        def gt(tag):
            el = rec.find(tag)
            return el.text.strip() if el is not None and el.text else ''
        
        last_name = gt('LastName') or gt('Entity')
        given_name = gt('GivenName')
        
        full_name = f"{given_name} {last_name}".strip() if given_name else last_name
        if not full_name:
            return None
        
        entity_type = 'person' if given_name else 'company'
        
        return {
            'source_id': gt('Item') or full_name[:80],
            'entity_type': entity_type,
            'full_name': full_name,
            'first_name': given_name,
            'last_name': last_name,
            'aliases': [gt('Aliases')] if gt('Aliases') else [],
            'identifiers': [],
            'nationalities': [],
            'addresses': [],
            'birth_dates': [gt('DateOfBirth')] if gt('DateOfBirth') else [],
            'birth_places': [],
            'programs': [gt('Schedule') or 'Canada SEMA'],
            'reasons': gt('Title'),
            'legal_basis': 'Special Economic Measures Act (SEMA)',
            'source_url': 'https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/consolidated-consolide.aspx',
        }


# ─────────────────────────────────────────────
# Australia DFAT Consolidated Sanctions
# ─────────────────────────────────────────────
class AustraliaCrawler(BaseCrawler):
    SOURCE_NAME = "australia_dfat"
    SOURCE_URL = "https://www.dfat.gov.au/sites/default/files/regulation8_consolidated.csv"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            text = raw_data.decode('utf-8-sig')
        except:
            text = raw_data.decode('latin-1')
        
        reader = csv.DictReader(io.StringIO(text))
        
        for row in reader:
            name = row.get('Name of Individual or Entity', '') or row.get('name', '')
            if not name:
                continue
            
            name_type = row.get('Name Type', '')
            entity_type = 'person' if 'individual' in (row.get('Type', '') or '').lower() else 'company'
            
            entities.append({
                'source_id': row.get('Reference', '') or name[:80],
                'entity_type': entity_type,
                'full_name': name,
                'first_name': '',
                'last_name': name,
                'aliases': [],
                'identifiers': [],
                'nationalities': [row.get('Citizenship', '')] if row.get('Citizenship') else [],
                'addresses': [row.get('Address', '')] if row.get('Address') else [],
                'birth_dates': [row.get('Date of Birth', '')] if row.get('Date of Birth') else [],
                'birth_places': [row.get('Place of Birth', '')] if row.get('Place of Birth') else [],
                'programs': [row.get('Committees', '') or 'Australia Sanctions'],
                'reasons': row.get('Additional Information', ''),
                'legal_basis': 'Australian Autonomous Sanctions Act 2011',
                'source_url': 'https://www.dfat.gov.au/international-relations/security/sanctions/consolidated-list',
            })
        
        return entities


# ─────────────────────────────────────────────
# Switzerland SECO
# ─────────────────────────────────────────────
class SECOCrawler(BaseCrawler):
    SOURCE_NAME = "swiss_seco"
    SOURCE_URL = "https://www.sesam.search.admin.ch/sesam-search-web/pages/downloadXmlGesamtliste.xhtml?lang=en&action=downloadXmlGesamtlisteAction"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            root = ET.fromstring(raw_data)
        except ET.ParseError:
            logger.error("[swiss_seco] XML parse error")
            return entities
        
        ns_uri = ''
        if root.tag.startswith('{'):
            ns_uri = root.tag.split('}')[0] + '}'
        
        for person in root.iter(f'{ns_uri}person') if ns_uri else root.iter('person'):
            entity = self._parse_person(person, ns_uri)
            if entity:
                entities.append(entity)
        
        for enterprise in root.iter(f'{ns_uri}enterprise') if ns_uri else root.iter('enterprise'):
            entity = self._parse_enterprise(enterprise, ns_uri)
            if entity:
                entities.append(entity)
        
        return entities
    
    def _get_text(self, el, tag, ns='', default=''):
        found = el.find(f'{ns}{tag}')
        if found is None:
            found = el.find(tag)
        return found.text.strip() if found is not None and found.text else default
    
    def _parse_person(self, el, ns) -> dict:
        ssid = el.get('ssid', '') or self._get_text(el, 'ssid', ns)
        
        first = self._get_text(el, 'firstname', ns)
        last = self._get_text(el, 'lastname', ns)
        full_name = f"{first} {last}".strip()
        
        if not full_name:
            return None
        
        return {
            'source_id': ssid or full_name[:80],
            'entity_type': 'person',
            'full_name': full_name,
            'first_name': first,
            'last_name': last,
            'aliases': [],
            'identifiers': [],
            'nationalities': [self._get_text(el, 'nationality', ns)] if self._get_text(el, 'nationality', ns) else [],
            'addresses': [],
            'birth_dates': [self._get_text(el, 'dateOfBirth', ns)] if self._get_text(el, 'dateOfBirth', ns) else [],
            'birth_places': [],
            'programs': ['Swiss SECO Sanctions'],
            'reasons': self._get_text(el, 'justification', ns),
            'legal_basis': 'Swiss Federal Act on the Implementation of International Sanctions',
            'source_url': 'https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html',
        }
    
    def _parse_enterprise(self, el, ns) -> dict:
        ssid = el.get('ssid', '') or self._get_text(el, 'ssid', ns)
        name = self._get_text(el, 'name', ns)
        
        if not name:
            return None
        
        return {
            'source_id': ssid or name[:80],
            'entity_type': 'company',
            'full_name': name,
            'first_name': '',
            'last_name': name,
            'aliases': [],
            'identifiers': [],
            'nationalities': [],
            'addresses': [],
            'birth_dates': [],
            'birth_places': [],
            'programs': ['Swiss SECO Sanctions'],
            'reasons': self._get_text(el, 'justification', ns),
            'legal_basis': 'Swiss Federal Act on the Implementation of International Sanctions',
            'source_url': 'https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html',
        }


# ─────────────────────────────────────────────
# US BIS Denied Persons List
# ─────────────────────────────────────────────
class BISDeniedCrawler(BaseCrawler):
    SOURCE_NAME = "us_bis_denied"
    SOURCE_URL = "https://www.bis.doc.gov/dpl/dpl.txt"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=60)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        text = raw_data.decode('utf-8', errors='replace')
        
        for line in text.strip().split('\n'):
            if not line.strip() or line.startswith('#'):
                continue
            
            # Tab-separated: Name, Street, City, State, Country, Zip, ...
            parts = line.split('\t')
            if len(parts) < 1:
                continue
            
            name = parts[0].strip()
            if not name:
                continue
            
            address_parts = [p.strip() for p in parts[1:6] if p.strip()] if len(parts) > 1 else []
            
            entities.append({
                'source_id': name[:80],
                'entity_type': 'person',
                'full_name': name,
                'first_name': '',
                'last_name': name,
                'aliases': [],
                'identifiers': [],
                'nationalities': [parts[4].strip()] if len(parts) > 4 and parts[4].strip() else [],
                'addresses': [', '.join(address_parts)] if address_parts else [],
                'birth_dates': [],
                'birth_places': [],
                'programs': ['BIS Denied Persons List'],
                'reasons': 'Denied export privileges',
                'legal_basis': 'Export Administration Regulations (EAR)',
                'source_url': 'https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/denied-persons-list',
            })
        
        return entities


# ─────────────────────────────────────────────
# US SAM.gov Exclusions
# ─────────────────────────────────────────────
class SAMExclusionsCrawler(BaseCrawler):
    SOURCE_NAME = "us_sam"
    SOURCE_URL = "https://api.sam.gov/entity-information/v3/exclusions"
    API_KEY = ""  # Free API key from sam.gov - register at https://open.gsa.gov/api/sam-entity-management-api/
    
    def fetch(self) -> bytes:
        """SAM requires pagination. Returns empty, actual fetch in parse()."""
        return b""
    
    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        
        if not self.API_KEY:
            logger.warning("[us_sam] No API key configured. Register free at https://open.gsa.gov/. Skipping.")
            return entities
        
        # SAM API requires date range or other filters
        params = {
            'api_key': self.API_KEY,
            'exclusionType': 'Firm,Individual,Special Entity Designation,Vessel',
            'limit': 100,
            'offset': 0,
        }
        
        while True:
            try:
                resp = self.session.get(self.SOURCE_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                
                results = data.get('results', [])
                if not results:
                    break
                
                for record in results:
                    entity = self._parse_record(record)
                    if entity:
                        entities.append(entity)
                
                if len(results) < params['limit']:
                    break
                
                params['offset'] += params['limit']
                
            except Exception as e:
                logger.error(f"[us_sam] Error at offset {params['offset']}: {e}")
                break
        
        return entities
    
    def _parse_record(self, record: dict) -> dict:
        name = record.get('firm', '') or ''
        first = record.get('firstname', '') or ''
        last = record.get('lastname', '') or ''
        
        if first and last:
            full_name = f"{first} {last}"
            entity_type = 'person'
        elif name:
            full_name = name
            entity_type = 'company'
        else:
            return None
        
        return {
            'source_id': record.get('uniqueEntityId', '') or full_name[:80],
            'entity_type': entity_type,
            'full_name': full_name,
            'first_name': first,
            'last_name': last or name,
            'aliases': [],
            'identifiers': [{'type': 'DUNS', 'value': record.get('duns', '')}] if record.get('duns') else [],
            'nationalities': [record.get('country', '')] if record.get('country') else [],
            'addresses': [record.get('city', '') + ', ' + record.get('state', '')] if record.get('city') else [],
            'birth_dates': [],
            'birth_places': [],
            'listed_date': record.get('activeDateStart', ''),
            'delisted_date': record.get('activeDateEnd', ''),
            'programs': [record.get('exclusionType', 'SAM Exclusion')],
            'reasons': record.get('exclusionProgram', ''),
            'legal_basis': 'Federal Acquisition Regulation (FAR)',
            'source_url': 'https://sam.gov/content/exclusions',
        }
    
    def run(self):
        """Override for paginated source."""
        import time as _time
        start = _time.time()
        
        entities = self.parse(b"")
        
        from db.models import get_db, upsert_entity, update_source_status
        conn = get_db()
        for entity in entities:
            entity['source'] = self.SOURCE_NAME
            upsert_entity(conn, entity)
        
        duration = _time.time() - start
        update_source_status(conn, self.SOURCE_NAME, len(entities), duration)
        conn.commit()
        conn.close()
        
        logger.info(f"[{self.SOURCE_NAME}] Synced {len(entities)} entities in {duration:.1f}s")
        return len(entities)
