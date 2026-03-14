"""
Crawler for UN Security Council Consolidated Sanctions List.
Source: https://scsanctions.un.org/resources/xml/en/consolidated.xml
Format: XML
~1,000 entities
"""

import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler


class UNSCCrawler(BaseCrawler):
    SOURCE_NAME = "un_sc"
    SOURCE_URL = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
    
    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content
    
    def parse(self, raw_data: bytes) -> list[dict]:
        root = ET.fromstring(raw_data)
        entities = []
        
        # Parse individuals
        for individual in root.iter('INDIVIDUAL'):
            entity = self._parse_individual(individual)
            if entity:
                entities.append(entity)
        
        # Parse entities/organizations
        for org in root.iter('ENTITY'):
            entity = self._parse_entity(org)
            if entity:
                entities.append(entity)
        
        return entities
    
    def _get_text(self, element, tag, default=''):
        el = element.find(tag)
        return el.text.strip() if el is not None and el.text else default
    
    def _parse_individual(self, ind) -> dict:
        dataid = self._get_text(ind, 'DATAID')
        if not dataid:
            return None
        
        first = self._get_text(ind, 'FIRST_NAME')
        second = self._get_text(ind, 'SECOND_NAME')
        third = self._get_text(ind, 'THIRD_NAME')
        fourth = self._get_text(ind, 'FOURTH_NAME')
        
        name_parts = [p for p in [first, second, third, fourth] if p]
        full_name = ' '.join(name_parts)
        
        # Aliases
        aliases = []
        for aka in ind.iter('INDIVIDUAL_ALIAS'):
            alias_name = self._get_text(aka, 'ALIAS_NAME')
            if alias_name:
                aliases.append(alias_name)
        
        # Dates of birth
        birth_dates = []
        for dob in ind.iter('INDIVIDUAL_DATE_OF_BIRTH'):
            date = self._get_text(dob, 'DATE') or self._get_text(dob, 'YEAR')
            if date:
                birth_dates.append(date)
        
        # Places of birth
        birth_places = []
        for pob in ind.iter('INDIVIDUAL_PLACE_OF_BIRTH'):
            city = self._get_text(pob, 'CITY')
            country = self._get_text(pob, 'COUNTRY')
            place = ', '.join([p for p in [city, country] if p])
            if place:
                birth_places.append(place)
        
        # Nationalities
        nationalities = []
        for nat in ind.iter('NATIONALITY'):
            val = self._get_text(nat, 'VALUE')
            if val:
                nationalities.append(val)
        
        # Addresses
        addresses = []
        for addr in ind.iter('INDIVIDUAL_ADDRESS'):
            parts = []
            for field in ['STREET', 'CITY', 'STATE_PROVINCE', 'COUNTRY']:
                val = self._get_text(addr, field)
                if val:
                    parts.append(val)
            if parts:
                addresses.append(', '.join(parts))
        
        # Documents/IDs
        identifiers = []
        for doc in ind.iter('INDIVIDUAL_DOCUMENT'):
            doc_type = self._get_text(doc, 'TYPE_OF_DOCUMENT')
            doc_nr = self._get_text(doc, 'NUMBER')
            if doc_type and doc_nr:
                identifiers.append({'type': doc_type, 'value': doc_nr})
        
        # Listed date and UN list type
        listed_on = self._get_text(ind, 'LISTED_ON')
        comments = self._get_text(ind, 'COMMENTS1')
        ref_number = self._get_text(ind, 'REFERENCE_NUMBER')
        
        programs = []
        un_list = self._get_text(ind, 'UN_LIST_TYPE')
        if un_list:
            programs.append(un_list)
        
        return {
            'source_id': dataid,
            'entity_type': 'person',
            'full_name': full_name,
            'first_name': first or '',
            'last_name': ' '.join([p for p in [second, third, fourth] if p]) or first,
            'aliases': aliases,
            'identifiers': identifiers,
            'nationalities': nationalities,
            'addresses': addresses,
            'birth_dates': birth_dates,
            'birth_places': birth_places,
            'listed_date': listed_on,
            'programs': programs,
            'reasons': comments,
            'legal_basis': ref_number,
            'source_url': f"https://www.un.org/securitycouncil/sanctions/1267/aq_sanctions_list",
        }
    
    def _parse_entity(self, ent) -> dict:
        dataid = self._get_text(ent, 'DATAID')
        if not dataid:
            return None
        
        first_name = self._get_text(ent, 'FIRST_NAME')
        
        aliases = []
        for aka in ent.iter('ENTITY_ALIAS'):
            alias_name = self._get_text(aka, 'ALIAS_NAME')
            if alias_name:
                aliases.append(alias_name)
        
        addresses = []
        for addr in ent.iter('ENTITY_ADDRESS'):
            parts = []
            for field in ['STREET', 'CITY', 'STATE_PROVINCE', 'COUNTRY']:
                val = self._get_text(addr, field)
                if val:
                    parts.append(val)
            if parts:
                addresses.append(', '.join(parts))
        
        listed_on = self._get_text(ent, 'LISTED_ON')
        comments = self._get_text(ent, 'COMMENTS1')
        ref_number = self._get_text(ent, 'REFERENCE_NUMBER')
        
        programs = []
        un_list = self._get_text(ent, 'UN_LIST_TYPE')
        if un_list:
            programs.append(un_list)
        
        return {
            'source_id': dataid,
            'entity_type': 'company',
            'full_name': first_name,
            'first_name': '',
            'last_name': first_name,
            'aliases': aliases,
            'identifiers': [],
            'nationalities': [],
            'addresses': addresses,
            'birth_dates': [],
            'birth_places': [],
            'listed_date': listed_on,
            'programs': programs,
            'reasons': comments,
            'legal_basis': ref_number,
            'source_url': f"https://www.un.org/securitycouncil/sanctions/1267/aq_sanctions_list",
        }
