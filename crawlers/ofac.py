"""
Crawler for US OFAC Specially Designated Nationals (SDN) list.
Source: https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML
Format: XML
~17,000 entities
"""

import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler


class OFACCrawler(BaseCrawler):
    SOURCE_NAME = "ofac_sdn"
    SOURCE_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        root = ET.fromstring(raw_data)
        ns = {'sdn': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML'}
        
        # Try to detect namespace from root tag
        ns_uri = ''
        if root.tag.startswith('{'):
            ns_uri = root.tag.split('}')[0] + '}'
        
        entities = []
        
        # Find all sdnEntry elements
        for entry in root.iter():
            if entry.tag.endswith('sdnEntry') or entry.tag == 'sdnEntry':
                entity = self._parse_entry(entry, ns_uri)
                if entity:
                    entities.append(entity)
        
        return entities
    
    def _parse_entry(self, entry, ns_uri: str) -> dict:
        """Parse a single SDN entry."""
        
        def find_text(element, tag, default=''):
            """Find text in element, handling namespace."""
            el = element.find(f'{ns_uri}{tag}')
            if el is None:
                el = element.find(tag)
            return el.text.strip() if el is not None and el.text else default
        
        uid = find_text(entry, 'uid')
        if not uid:
            return None
        
        sdn_type = find_text(entry, 'sdnType', 'Individual')
        first_name = find_text(entry, 'firstName')
        last_name = find_text(entry, 'lastName')
        
        # Build full name
        if first_name and last_name:
            full_name = f"{first_name} {last_name}"
        else:
            full_name = last_name or first_name or ''
        
        # Parse programs
        programs = []
        program_list = entry.find(f'{ns_uri}programList') or entry.find('programList')
        if program_list is not None:
            for prog in program_list:
                if prog.text:
                    programs.append(prog.text.strip())
        
        # Parse aliases (akaList)
        aliases = []
        aka_list = entry.find(f'{ns_uri}akaList') or entry.find('akaList')
        if aka_list is not None:
            for aka in aka_list:
                aka_first = find_text(aka, 'firstName')
                aka_last = find_text(aka, 'lastName')
                if aka_first and aka_last:
                    aliases.append(f"{aka_first} {aka_last}")
                elif aka_last:
                    aliases.append(aka_last)
        
        # Parse IDs
        identifiers = []
        id_list = entry.find(f'{ns_uri}idList') or entry.find('idList')
        if id_list is not None:
            for id_entry in id_list:
                id_type = find_text(id_entry, 'idType')
                id_number = find_text(id_entry, 'idNumber')
                if id_type and id_number:
                    identifiers.append({'type': id_type, 'value': id_number})
        
        # Parse addresses
        addresses = []
        addr_list = entry.find(f'{ns_uri}addressList') or entry.find('addressList')
        if addr_list is not None:
            for addr in addr_list:
                parts = []
                for field in ['address1', 'address2', 'address3', 'city', 'stateOrProvince', 'country']:
                    val = find_text(addr, field)
                    if val:
                        parts.append(val)
                if parts:
                    addresses.append(', '.join(parts))
        
        # Parse nationalities and DOBs
        nationalities = []
        birth_dates = []
        birth_places = []
        
        # Nationality and DOB are in dateOfBirthList and nationalityList
        dob_list = entry.find(f'{ns_uri}dateOfBirthList') or entry.find('dateOfBirthList')
        if dob_list is not None:
            for dob in dob_list:
                dob_val = find_text(dob, 'dateOfBirth')
                if dob_val:
                    birth_dates.append(dob_val)
        
        nat_list = entry.find(f'{ns_uri}nationalityList') or entry.find('nationalityList')
        if nat_list is not None:
            for nat in nat_list:
                nat_val = find_text(nat, 'country')
                if nat_val:
                    nationalities.append(nat_val)
        
        # Parse remarks
        remarks = find_text(entry, 'remarks')
        
        entity_type = 'person' if sdn_type == 'Individual' else 'company'
        if sdn_type == 'Vessel':
            entity_type = 'vessel'
        elif sdn_type == 'Aircraft':
            entity_type = 'aircraft'
        
        return {
            'source_id': uid,
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
            'programs': programs,
            'reasons': remarks,
            'source_url': f"https://sanctionssearch.ofac.treas.gov/Details.aspx?id={uid}",
        }
