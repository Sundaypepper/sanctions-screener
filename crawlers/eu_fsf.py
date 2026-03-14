"""
Crawler for EU Financial Sanctions File (FSF).
Source: https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw
Format: XML
~2,500 entities
"""

import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler


class EUFSFCrawler(BaseCrawler):
    SOURCE_NAME = "eu_fsf"
    SOURCE_URL = "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        root = ET.fromstring(raw_data)
        entities = []
        
        # EU FSF uses namespaced XML
        ns_uri = ''
        if root.tag.startswith('{'):
            ns_uri = root.tag.split('}')[0] + '}'
        
        for entity_el in root.iter(f'{ns_uri}sanctionEntity'):
            entity = self._parse_entity(entity_el, ns_uri)
            if entity:
                entities.append(entity)
        
        return entities
    
    def _find_text(self, el, tag, ns, default=''):
        found = el.find(f'{ns}{tag}')
        if found is None:
            found = el.find(tag)
        return found.text.strip() if found is not None and found.text else default
    
    def _parse_entity(self, el, ns) -> dict:
        # Get logical ID
        log_id = el.get('logicalId', '') or el.get('designationDate', '')
        if not log_id:
            # Try to find any ID attribute
            log_id = str(hash(ET.tostring(el)))
        
        # Determine type
        subject_type = ''
        subject_el = el.find(f'{ns}subjectType') or el.find('subjectType')
        if subject_el is not None:
            code = subject_el.get('code', '') or (subject_el.text or '')
            subject_type = code.lower()
        
        entity_type = 'person' if 'person' in subject_type or 'individual' in subject_type else 'company'
        
        # Names
        full_name = ''
        first_name = ''
        last_name = ''
        aliases = []
        
        for name_alias in el.iter(f'{ns}nameAlias') if ns else el.iter('nameAlias'):
            whole_name = name_alias.get('wholeName', '')
            fn = name_alias.get('firstName', '')
            ln = name_alias.get('lastName', '') or name_alias.get('familyName', '')
            is_strong = name_alias.get('strong', 'true')
            
            if not full_name and whole_name:
                full_name = whole_name
                first_name = fn
                last_name = ln
            elif whole_name:
                aliases.append(whole_name)
            
            if not full_name and fn and ln:
                full_name = f"{fn} {ln}"
                first_name = fn
                last_name = ln
        
        if not full_name:
            return None
        
        # Birth dates
        birth_dates = []
        for bd in el.iter(f'{ns}birthdate') if ns else el.iter('birthdate'):
            date = bd.get('birthdate', '') or bd.get('year', '')
            if date:
                birth_dates.append(date)
        
        # Birth places
        birth_places = []
        for bd in el.iter(f'{ns}birthdate') if ns else el.iter('birthdate'):
            city = bd.get('city', '')
            country = bd.get('countryDescription', '')
            place = ', '.join([p for p in [city, country] if p])
            if place:
                birth_places.append(place)
        
        # Addresses
        addresses = []
        for addr in el.iter(f'{ns}address') if ns else el.iter('address'):
            parts = []
            for field in ['street', 'city', 'zipCode', 'countryDescription']:
                val = addr.get(field, '')
                if val:
                    parts.append(val)
            if parts:
                addresses.append(', '.join(parts))
        
        # Identifiers (passports, etc.)
        identifiers = []
        for ident in el.iter(f'{ns}identification') if ns else el.iter('identification'):
            id_type = ident.get('identificationTypeDescription', '') or ident.get('identificationTypeCode', '')
            id_nr = ident.get('number', '') or ident.get('diplomaticPassport', '')
            if id_type and id_nr:
                identifiers.append({'type': id_type, 'value': id_nr})
        
        # Nationalities (citizenships)
        nationalities = []
        for cit in el.iter(f'{ns}citizenship') if ns else el.iter('citizenship'):
            country = cit.get('countryDescription', '')
            if country:
                nationalities.append(country)
        
        # Regulation info
        programs = []
        regulation = el.get('euReferenceNumber', '') or el.get('unitedNationId', '')
        if regulation:
            programs.append(regulation)
        
        # Designation details
        designation_date = el.get('designationDate', '')
        remark = ''
        for rem in el.iter(f'{ns}remark') if ns else el.iter('remark'):
            if rem.text:
                remark = rem.text.strip()
                break
        
        return {
            'source_id': str(log_id),
            'entity_type': entity_type,
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name,
            'aliases': aliases,
            'identifiers': identifiers,
            'nationalities': nationalities,
            'addresses': addresses,
            'birth_dates': birth_dates,
            'birth_places': birth_places,
            'listed_date': designation_date,
            'programs': programs,
            'reasons': remark,
            'legal_basis': 'EU Financial Sanctions',
            'source_url': 'https://www.sanctionsmap.eu/',
        }
