"""
Additional crawlers for: UK OFSI, World Bank, Canada, Australia DFAT,
Switzerland SECO, US BIS Denied Persons, US SAM Exclusions.
"""

import csv
import io
import json
import time
import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler, logger


# ─────────────────────────────────────────────
# UK OFSI - Office of Financial Sanctions
# ─────────────────────────────────────────────
class UKOFSICrawler(BaseCrawler):
    SOURCE_NAME = "uk_ofsi"
    # OFSI Consolidated List retired 28 Jan 2026; replaced by UK Sanctions List
    SOURCE_URL = "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.csv"

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

        # FCDO CSV has "Report Date: ..." as first line, skip it
        lines = text.split('\n')
        if lines and lines[0].startswith('Report Date'):
            text = '\n'.join(lines[1:])

        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            name6 = row.get('Name 6', '') or ''
            name1 = row.get('Name 1', '') or ''
            name2 = row.get('Name 2', '') or ''
            name3 = row.get('Name 3', '') or ''

            # "Designation Type" column: Individual, Entity, Ship
            group_type = (row.get('Designation Type', '')
                          or row.get('Individual, Entity, Ship', '')
                          or row.get('Group Type', '') or '')

            # Build name
            name_parts = [p for p in [name6, name1, name2, name3] if p and p.strip()]
            full_name = ' '.join(name_parts).strip()

            if not full_name:
                continue

            uid = (row.get('Unique ID', '')
                   or row.get('OFSI Group ID', '')
                   or row.get('Group ID', '')
                   or str(hash(full_name)))

            entity_type = 'person' if 'individual' in group_type.lower() else 'company'

            # DOB
            birth_dates = []
            dob = row.get('D.O.B', '') or row.get('DOB', '') or ''
            if dob:
                birth_dates.append(dob)

            # Nationality
            nationalities = []
            nat = row.get('Nationality(/ies)', '') or row.get('Nationality', '') or ''
            if nat:
                nationalities.append(nat)

            # Regime
            regime = row.get('Regime Name', '') or row.get('Regime', '') or ''

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
                'reasons': row.get('Other Information', '') or '',
                'legal_basis': 'UK Sanctions and Anti-Money Laundering Act 2018',
                'source_url': 'https://www.gov.uk/government/publications/the-uk-sanctions-list',
            })

        return entities


# ─────────────────────────────────────────────
# World Bank Debarment List
# ─────────────────────────────────────────────
class WorldBankCrawler(BaseCrawler):
    SOURCE_NAME = "worldbank"
    # World Bank API requires subscription key; using OpenSanctions mirror instead
    SOURCE_URL = "https://data.opensanctions.org/datasets/latest/worldbank_debarred/entities.ftm.json"

    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        lines = raw_data.decode('utf-8').strip().split('\n')

        for line in lines:
            try:
                record = json.loads(line)
            except:
                continue

            schema = record.get('schema', '')
            if schema not in ('LegalEntity', 'Person', 'Company', 'Organization'):
                continue

            props = record.get('properties', {})
            name = (props.get('name', [''])[0] if props.get('name') else '')
            if not name:
                continue

            country = props.get('country', [''])[0] if props.get('country') else ''
            address = props.get('address', [''])[0] if props.get('address') else ''

            entity_type = 'person' if schema == 'Person' else 'company'

            en�───────────────────────
# World Bank Debarment List
# ─────────────────────────────────────────────
class WorldBankCrawler(BaseCrawler):
    SOURCE_NAME = "worldbank"
    # World Bank API requires subscription key; using OpenSanctions mirror instead
    SOURCE_URL = "https://data.opensanctions.org/datasets/latest/worldbank_debarred/entities.ftm.json"

    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        lines = raw_data.decode('utf-8').strip().split('\n')

        for line in lines:
            try:
                record = json.loads(line)
            except:
                continue

            schema = record.get('schema', '')
            if schema not in ('LegalEntity', 'Person', 'Company', 'Organization'):
                continue

            props = record.get('properties', {})
            name = (props.get('name', [''])[0] if props.get('name') else '')
            if not name:
                continue

            country = props.get('country', [''])[0] if props.get('country') else ''
            address = props.get('address', [''])[0] if props.get('address') else ''

            entity_type = 'person' if schema == 'Person' else 'company'

            entities.append({
                'source_id': record.get('id', name[:100]),
                'entity_type': entity_type,
                'full_name': name,
                'first_name': props.get('firstName', [''])[0] if props.get('firstName') else '',
                'last_name': props.get('lastName', [''])[0] if props.get('lastName') else name,
                'aliases': [],
                'identifiers': [],
                'nationalities': [country.upper()] if country else [],
                'addresses': [address] if address else [],
                'birth_dates': [],
                'birth_places': [],
                'programs': ['World Bank Debarment'],
                'reasons': '',
                'legal_basis': 'World Bank Sanctions Procedures',
                'source_url': 'https://www.worldbank.org/en/projects-operations/procurement/debarred-firms',
            })

        return entities


# ─────────────────────────────────────────────
# Canada - Consolidated Autonomous Sanctions
# ─────────────────────────────────────────────
class CanadaCrawler(BaseCrawler):
    SOURCE_NAME = "canada_sema"
    SOURCE_URL = "https://www.international.gc.ca/world-monde/assets/office_docs/international_relations-relations_internationales/sanctions/sema-lmes.xml"

    def fetch(self) -> bytes:
        # Canadian site can be slow; retry with longer timeout
        for attempt in range(3):
            try:
                resp = self.session.get(self.SOURCE_URL, timeout=180)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"[canada] Attempt {attempt+1} failed: {e}, retrying...")
                    time.sleep(5)
                else:
                    raise
        return b""

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        if not raw_data:
            return entities
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
    # XLS file (the CSV version is no longer reliably available)
    SOURCE_URL = "https://www.dfat.gov.au/sites/default/files/regulation8_consolidated_2.xls"

    def fetch(self) -> bytes:
        for attempt in range(3):
            try:
                resp = self.session.get(self.SOURCE_URL, timeout=180)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"[australia] Attempt {attempt+1} failed: {e}, retrying...")
                    time.sleep(5)
                else:
                    raise
        return b""

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        if not raw_data:
            return entities

        try:
            import xlrd
            workbook = xlrd.open_workbook(file_contents=raw_data)
            sheet = workbook.sheet_by_index(0)

            # Find header row
            headers = [str(sheet.cell_value(0, col)).strip() for col in range(sheet.ncols)]

            for row_idx in range(1, sheet.nrows):
                row = {}
                for col_idx in range(sheet.ncols):
                    row[headers[col_idx]] = str(sheet.cell_value(row_idx, col_idx)).strip()

                name = (row.get('Name of Individual or Entity', '')
                        or row.get('FULL_NAME', '')
                        or row.get('name', '') or '')
                if not name:
                    continue

                entity_type = 'person' if 'individual' in (row.get('Type', '') or row.get('type', '')).lower() else 'company'

                entities.append({
                    'source_id': row.get('Reference', '') or row.get('reference', '') or name[:80],
                    'entity_type': entity_type,
                    'full_name': n
            logger.error(f"[australia] XLS parse error: {e}")

        return entities

    def _parse_csv(self, raw_data: bytes) -> list[dict]:
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
        resp = self.session.get(self.SOURCE_URL, timeout=180, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            root = ET.fromstring(raw_data)
        except ET.ParseError:
            logger.error("[swiss_seco] XML parse error")
            return entities

        # New SECO format (since Dec 2023): <swiss-sanctions-list> -> <sanctions-program> -> <target>
        # Each target contains <individual> or <entity>
        for target in root.iter('target'):
            ssid = target.get('ssid', '')

            individual = target.find('individual')
            if individual is not None:
                entity = self._parse_individual(individual, ssid)
                if entity:
                    entities.append(entity)
                continue

            entity_el = target.find('entity')
            if entity_el is not None:
                entity = self._parse_entity_el(entity_el, ssid)
                if entity:
                    entities.append(entity)

        return entities

    def _get_primary_name_parts(self, el):
        """Extract name parts from individual/entity identity."""
        family = ''
        given = ''
        aliases = []

        # Find main identity
        for identity in el.iter('identity'):
            if identity.get('main') != 'true':
                continue

            for name_el in identity.iter('name'):
                name_type = name_el.get('name-type', '')

                if name_type == 'primary-name':
                    for part in name_el.iter('name-part'):
                        part_type = part.get('name-part-type', '')
                        value_el = part.find('value')
                        value = value_el.text.strip() if value_el is not None and value_el.text else ''

                        if part_type == 'family-name':
                            family = value
                        elif part_type == 'given-name':
                            given = value
                        elif part_type == 'entity-name':
                            family = value

                elif name_type in ('alias', 'also-known-as', 'a.k.a.'):
                    parts = []
                    for part in name_el.iter('name-part'):
                        value_el = part.find('value')
                        if value_el is not None and value_el.text:
                            parts.append(value_el.text.strip())
                    if parts:
                        aliases.append(' '.join(parts))

            break  # Only process main identity

        return family, given, aliases

    def _get_dob(self, el):
        """Extract date of birth from identity."""
        for identity in el.iter('identity'):
            if identity.get('main') != 'true':
                continue
            for dmy in identity.iter('day-month-year'):
                day = dmy.get('day', '')
                month = dmy.get('month', '')
                year = dmy.get('year', '')
                if year:
                    parts = []
                    if day:
                        parts.append(day.zfill(2))
                    if month:
                        parts.append(month.zfill(2))
                    parts.append(year)
                    return '/'.join(parts)
            break
        return ''

    def _get_justification(self, el):
        """Extract justification text."""
        just_el = el.find('justification')
        if just_el is not None and just_el.text:
            return just_el.text.strip()
        return ''

    def _parse_individual(self, el, ssid) -> dict:
        family, given, aliases = self._get_primary_name_parts(el)
        full_name = f"{given} {family}".strip()
        if not full_name:
            return None

        dob = self._get_dob(el)

        return {
            'source_id': ssid or full_name[:80],
            'entity_type': 'person',
            'full_name': full_name,
            'first_name': given,
            'last_name': family,
            'aliases': aliases,
            'identifiers': [],
            'nationalities': [],
            'addresses': [],
            'birth_dates': [dob] if dob else [],
            'birth_places': [],
            'programs': ['Swiss SECO Sanctions'],
            'reasons': self._get_justification(el),
            'legal_basis': 'Swiss Federal Act on the Implementation of International Sanctions',
            'source_url': 'https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html',
        }

    def _parse_entity_el(self, el, ssid) -> dict:
        family, given, aliases = self._get_primary_name_parts(el)
        name = family or given
        if not name:
            return None

        return {
            'source_id': ssid or name[:80],
            'entity_type': 'company',
            'full_name': name,
            'first_name': '',
            'last_name': name,
            'aliases': aliases,
            'identifiers': [],
            'nationalities': [],
            'addresses': [],
            'birth_dates': [],
            'birth_places': [],
            'programs': ['Swiss SECO Sanctions'],
            'reasons': self._get_justification(el),
            'legal_basis': 'Swiss Federal Act on the Implementation of International Sanctions',
            'source_url': 'https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html',
        }


# ─────────────────────────────────────────────
# US BIS Denied Persons List
# ─────────────────────────────────────────────
class BISDeniedCrawler(BaseCrawler):
    SOURCE_NAME = "us_bis_denied"
    SOURCE_URL = "https://www.bis.gov/licensing/end-user-guidance/denied-persons-list-dpl"

    def fetch(self) -> bytes:
        """Find the current CSV download link from the BIS page."""
        import re

        # First get the page to find the CSV link
        resp = self.session.get(self.SOURCE_URL, timeout=60, verify=False)
        resp.raise_for_status()

        # Find CSV link in the page
        csv_match = re.search(r'https://media\.bis\.gov/[^"\'>\s]*\.csv', resp.text)
        if csv_match:
            csv_url = csv_match.group(0)
            logger.info(f"[us_bis_denied] Found CSV: {csv_url}")
            resp2 = self.session.get(csv_url, timeout=60, verify=False)
            resp2.raise_for_status()
            return resp2.content

        # Fallback: try known URL pattern
        resp2 = self.session.get("https://www.bis.doc.gov/dpl/dpl.txt", timeout=60, verify=False)
        resp2.raise_for_status()
        return resp2.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        text = raw_data.decode('utf-8-sig', errors='replace')

        # Try CSV format first (new BIS format)
        if 'Name_and_Address' in text or 'Effective_Date' in text:
            return self._parse_csv(text)

        # Fallback: tab-separated (old format)
        return self._parse_tsv(text)

    def _parse_csv(self, text: str) -> list[dict]:
        """Parse the new BIS CSV format: Name_and_Address,Effective_Date,..."""
        entities = []
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            name_addr = row.get('Name_and_Address', '') or ''
            if not name_addr:
                continue

            # Name and address are combined, comma-separated
            parts = [p.strip() for p in name_addr.split(',')]
            name = parts[0] if parts else ''
            if not name:
                continue

            address_parts = [p for p in parts[1:] if p]
            country = ''
            for p in reversed(address_parts):
                if len(p) == 2 and p.isalpha():
                    country = p
                    break

            effective = row.get('Effective_Date', '') or ''
            expiration = row.get('Expiration_Date', '') or ''

            entities.append({
                'source_id': name[:80],
                'entity_type': 'person',
                'full_name': name,
                'first_name': '',
                'last_name': name,
                'aliases': [],
                'identifiers': [],
                'nationalities': [country] if country else [],
                'addresses': [', '.join(address_parts)] if address_parts else [],
                'birth_dates': [],
                'birth_places': [],
                'programs': ['BIS Denied Persons List'],
                'reasons': f"Denied export privileges (effective {effective})" if effective else 'Denied export privileges',
                'legal_basis': 'Export Administration Regulations (EAR)',
                'source_url': 'https://www.bis.gov/licensing/end-user-guidance/denied-persons-list-dpl',
            })

        return entities

    def _parse_tsv(self, text: str) -> list[dict]:
        """Parse the old tab-separated BIS format."""
        entities = []
        for line in text.strip().split('\n'):
            if not line.strip() or line.startswith('#'):
                continue

            parts = line.split('\t')
            name = parts[0].strip() if parts else ''
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
                'source_url': 'https://www.bis.gov/licensing/end-user-guidance/denied-persons-list-dpl',
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
            'programs': [record.get('exclusionType', 'SAM Exclusion')],
            'reasons': record.get('exclusionProgram', ''),
            'legal_basis': 'Federal Acquisition Regulation (FAR)',
            'source_url': 'https://sam.gov/content/exclusions',
        }

    # Uses BaseCrawler.run()
