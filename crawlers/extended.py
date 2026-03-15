"""
Extended crawlers for: France Trésor (Gel des avoirs),
OFAC Consolidated Non-SDN List, OHCHR Settlement Database.
"""

import json
import xml.etree.ElementTree as ET
from crawlers.base import BaseCrawler, logger


# ─────────────────────────────────────────────
# France Trésor - Registre national des gels
# ─────────────────────────────────────────────
class FranceTresorCrawler(BaseCrawler):
    """
    French national asset freezing register.
    Free JSON API, no key required. ~6000 entities.
    https://gels-avoirs.dgtresor.gouv.fr/
    """
    SOURCE_NAME = "france_tresor"
    SOURCE_URL = "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/derniere-publication-flux-json"

    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.error("[france_tresor] JSON parse error")
            return entities

        records = data.get("Publications", {}).get("PublicationDetail", [])

        for record in records:
            entity = self._parse_record(record)
            if entity:
                entities.append(entity)

        return entities

    def _parse_record(self, rec: dict) -> dict:
        id_registre = str(rec.get("IdRegistre", ""))
        nature = rec.get("Nature", "")
        nom = rec.get("Nom", "")

        if not nom:
            return None

        # Parse detail fields
        details = {d["TypeChamp"]: d.get("Valeur", []) for d in rec.get("RegistreDetail", [])}

        # Name
        prenom = ""
        prenom_vals = details.get("PRENOM", [])
        if prenom_vals and isinstance(prenom_vals[0], dict):
            prenom = prenom_vals[0].get("Prenom", "")

        full_name = f"{prenom} {nom}".strip() if prenom else nom

        # Entity type
        if "physique" in nature.lower():
            entity_type = "person"
        elif "morale" in nature.lower():
            entity_type = "company"
        elif "navire" in nature.lower():
            entity_type = "vessel"
        else:
            entity_type = "unknown"

        # Aliases
        aliases = []
        for a in details.get("ALIAS", []):
            if isinstance(a, dict) and a.get("Alias"):
                aliases.append(a["Alias"])

        # Birth dates
        birth_dates = []
        for bd in details.get("DATE_DE_NAISSANCE", []):
            if isinstance(bd, dict):
                parts = [bd.get("Jour", ""), bd.get("Mois", ""), bd.get("Annee", "")]
                date_str = "/".join(p for p in parts if p)
                if date_str:
                    birth_dates.append(date_str)

        # Birth places
        birth_places = []
        for bp in details.get("LIEU_DE_NAISSANCE", []):
            if isinstance(bp, dict):
                lieu = bp.get("Lieu", "")
                pays = bp.get("Pays", "")
                place = f"{lieu}, {pays}".strip(", ") if lieu or pays else ""
                if place:
                    birth_places.append(place)

        # Nationalities
        nationalities = []
        for n in details.get("NATIONALITE", []):
            if isinstance(n, dict) and n.get("Nationalite"):
                nationalities.append(n["Nationalite"])

        # Reasons / motifs
        reasons = ""
        for m in details.get("MOTIFS", []):
            if isinstance(m, dict) and m.get("Motifs"):
                reasons = m["Motifs"][:500]
                break

        # Legal basis
        legal_basis_parts = []
        for fb in details.get("FONDEMENT_JURIDIQUE", []):
            if isinstance(fb, dict) and fb.get("FondementJuridiqueLabel"):
                legal_basis_parts.append(fb["FondementJuridiqueLabel"])
        legal_basis = "; ".join(legal_basis_parts[:3]) if legal_basis_parts else "French National Asset Freezing"

        # Identifiers
        identifiers = []
        for ident in details.get("IDENTIFICATION", []):
            if isinstance(ident, dict) and ident.get("Identification"):
                identifiers.append({
                    "type": "identification",
                    "value": ident["Identification"],
                })

        # Addresses
        addresses = []
        for addr in details.get("ADRESSE", []):
            if isinstance(addr, dict) and addr.get("Adresse"):
                addresses.append(addr["Adresse"])

        return {
            "source_id": id_registre,
            "entity_type": entity_type,
            "full_name": full_name,
            "first_name": prenom,
            "last_name": nom,
            "aliases": aliases,
            "identifiers": identifiers,
            "nationalities": nationalities,
            "addresses": addresses,
            "birth_dates": birth_dates,
            "birth_places": birth_places,
            "programs": ["France Gel des Avoirs"],
            "reasons": reasons,
            "legal_basis": legal_basis,
            "source_url": "https://gels-avoirs.dgtresor.gouv.fr/",
        }


# ─────────────────────────────────────────────
# OFAC Consolidated Non-SDN List
# ─────────────────────────────────────────────
class OFACConsolidatedCrawler(BaseCrawler):
    """
    OFAC Consolidated (non-SDN) sanctions list.
    Includes SSI, FSE, NS-MBS, CAPTA, and other non-SDN programs.
    Free XML download. ~440 entities.
    """
    SOURCE_NAME = "ofac_cons"
    SOURCE_URL = "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml"

    def fetch(self) -> bytes:
        resp = self.session.get(self.SOURCE_URL, timeout=120)
        resp.raise_for_status()
        return resp.content

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []
        try:
            root = ET.fromstring(raw_data)
        except ET.ParseError:
            logger.error("[ofac_cons] XML parse error")
            return entities

        # Detect namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        for entry in root.iter(f"{ns}sdnEntry"):
            entity = self._parse_entry(entry, ns)
            if entity:
                entities.append(entity)

        return entities

    def _parse_entry(self, entry, ns: str) -> dict:
        def gt(tag):
            el = entry.find(f"{ns}{tag}")
            return el.text.strip() if el is not None and el.text else ""

        uid = gt("uid")
        first_name = gt("firstName")
        last_name = gt("lastName")
        sdn_type = gt("sdnType")

        full_name = f"{first_name} {last_name}".strip() if first_name else last_name
        if not full_name:
            return None

        entity_type = "person" if "individual" in sdn_type.lower() else "company"

        # Programs
        programs = []
        program_list = entry.find(f"{ns}programList")
        if program_list is not None:
            for prog in program_list.iter(f"{ns}program"):
                if prog.text and prog.text.strip():
                    programs.append(prog.text.strip())

        # Aliases
        aliases = []
        aka_list = entry.find(f"{ns}akaList")
        if aka_list is not None:
            for aka in aka_list.iter(f"{ns}aka"):
                aka_first = ""
                aka_last = ""
                el = aka.find(f"{ns}firstName")
                if el is not None and el.text:
                    aka_first = el.text.strip()
                el = aka.find(f"{ns}lastName")
                if el is not None and el.text:
                    aka_last = el.text.strip()
                aka_name = f"{aka_first} {aka_last}".strip()
                if aka_name:
                    aliases.append(aka_name)

        # IDs
        identifiers = []
        id_list = entry.find(f"{ns}idList")
        if id_list is not None:
            for id_entry in id_list.iter(f"{ns}id"):
                id_type_el = id_entry.find(f"{ns}idType")
                id_num_el = id_entry.find(f"{ns}idNumber")
                if id_type_el is not None and id_num_el is not None:
                    id_type = id_type_el.text.strip() if id_type_el.text else ""
                    id_num = id_num_el.text.strip() if id_num_el.text else ""
                    if id_type and id_num:
                        identifiers.append({"type": id_type, "value": id_num})

        # Addresses
        addresses = []
        addr_list = entry.find(f"{ns}addressList")
        if addr_list is not None:
            for addr in addr_list.iter(f"{ns}address"):
                parts = []
                for tag in ["address1", "address2", "address3", "city", "stateOrProvince", "country"]:
                    el = addr.find(f"{ns}{tag}")
                    if el is not None and el.text and el.text.strip():
                        parts.append(el.text.strip())
                if parts:
                    addresses.append(", ".join(parts))

        # Nationalities
        nationalities = []
        nat_list = entry.find(f"{ns}nationalityList")
        if nat_list is not None:
            for nat in nat_list.iter(f"{ns}nationality"):
                country_el = nat.find(f"{ns}country")
                if country_el is not None and country_el.text:
                    nationalities.append(country_el.text.strip())

        # DOB
        birth_dates = []
        dob_list = entry.find(f"{ns}dateOfBirthList")
        if dob_list is not None:
            for dob in dob_list.iter(f"{ns}dateOfBirthItem"):
                dob_el = dob.find(f"{ns}dateOfBirth")
                if dob_el is not None and dob_el.text:
                    birth_dates.append(dob_el.text.strip())

        # Place of birth
        birth_places = []
        pob_list = entry.find(f"{ns}placeOfBirthList")
        if pob_list is not None:
            for pob in pob_list.iter(f"{ns}placeOfBirthItem"):
                pob_el = pob.find(f"{ns}placeOfBirth")
                if pob_el is not None and pob_el.text:
                    birth_places.append(pob_el.text.strip())

        return {
            "source_id": uid,
            "entity_type": entity_type,
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "aliases": aliases,
            "identifiers": identifiers,
            "nationalities": nationalities,
            "addresses": addresses,
            "birth_dates": birth_dates,
            "birth_places": birth_places,
            "programs": programs if programs else ["OFAC Non-SDN"],
            "reasons": "",
            "legal_basis": "International Emergency Economic Powers Act (IEEPA)",
            "source_url": "https://home.treasury.gov/policy-issues/financial-sanctions/consolidated-sanctions-list-non-sdn-lists",
        }


# ─────────────────────────────────────────────
# OHCHR - UN Settlement Business Database
# ─────────────────────────────────────────────
class OHCHRSettlementCrawler(BaseCrawler):
    """
    UN OHCHR database of businesses involved in Israeli settlements
    in the occupied Palestinian territory.
    ~158 companies. Data from OHCHR reports (HTML page scrape).
    """
    SOURCE_NAME = "ohchr_settlement"
    SOURCE_URL = "https://www.ohchr.org/en/hr-bodies/hrc/regular-sessions/session31/database-hrc3136"

    # From OHCHR report A/HRC/60/19 (September 2025).
    # 158 companies from 11 countries. Updated when OHCHR publishes new reports.
    COMPANIES = [
        ("Ackerstein Industries", "Israel"),
        ("Ahava Dead Sea Laboratories", "Israel"),
        ("Alon Blue Square Israel", "Israel"),
        ("Altice Group", "Israel"),
        ("American Israeli Gas Corporation", "Israel"),
        ("Amir Marketing and Investments in Agriculture Ltd", "Israel"),
        ("Amot Investments", "Israel"),
        ("Anglo Saxon Real Estate Agency", "Israel"),
        ("Archivists", "Israel"),
        ("Ashtrom", "Israel"),
        ("Ashtrom Industries", "Israel"),
        ("Ashtrom Residential Development", "Israel"),
        ("A. Barkan and Partners", "Israel"),
        ("B. Gaon Holdings", "Israel"),
        ("Bank Leumi Le-Israel", "Israel"),
        ("Bank of Jerusalem", "Israel"),
        ("Bar Amana Buildings Construction & Development", "Israel"),
        ("Baran Group", "Israel"),
        ("Bardarian Brothers", "Israel"),
        ("Beit Haarchiv", "Israel"),
        ("Benny and Tzvika Group", "Israel"),
        ("Bezeq", "Israel"),
        ("Boneich Construction Development & Investments", "Israel"),
        ("Booking.com", "United States"),
        ("Brothers Hasid Construction Contracting Company", "Israel"),
        ("C. Mer Group", "Israel"),
        ("Cafe Cafe", "Israel"),
        ("Caliber 3", "Israel"),
        ("Cellcom Israel", "Israel"),
        ("Cherriessa", "Israel"),
        ("CIM Lustigman", "Israel"),
        ("CityBook Services", "Israel"),
        ("D.N. Kol Gader", "Israel"),
        ("Dalia Elispur Construction Contracting Company 1972", "Israel"),
        ("Dan Public Transportation Company", "Israel"),
        ("Danya Cebus", "Israel"),
        ("Davidov Garages", "Israel"),
        ("Db Billiards", "Israel"),
        ("Delek", "Israel"),
        ("Delta Galil", "Israel"),
        ("Delta Israel Brands", "Israel"),
        ("Dor Alon", "Israel"),
        ("Egis Group", "France"),
        ("Egis Rail", "France"),
        ("Egged Cooperative", "Israel"),
        ("Einav Hahetz 1965", "Israel"),
        ("Electra", "Israel"),
        ("Electra Afikim", "Israel"),
        ("Elyakim Ben-Ari", "Israel"),
        ("EPR Systems", "Israel"),
        ("E.T. Legal Services", "Israel"),
        ("Euro-Israel", "Israel"),
        ("Extal", "Israel"),
        ("Export Investment Co.", "Israel"),
        ("Extra Retail Group", "Israel"),
        ("Field Produce", "Israel"),
        ("Field Produce Marketing", "Israel"),
        ("First International Bank of Israel", "Israel"),
        ("Fosun International", "China"),
        ("Gadish Engineering Company", "Israel"),
        ("Galnor Construction and Development", "Israel"),
        ("Galshan Shvakim", "Israel"),
        ("Geo-Da Lands", "Israel"),
        ("Greenkote PLC", "United Kingdom"),
        ("Grupo ACS", "Spain"),
        ("Hadar Group", "Israel"),
        ("Haim Zaken Construction & Investments", "Israel"),
        ("Hamat Group", "Israel"),
        ("Harsa Studio Sanitaryware Manufacturers", "Israel"),
        ("HOT Mobile", "Israel"),
        ("Hot Telecommunications Systems", "Israel"),
        ("Impact Property Development", "Israel"),
        ("Ingenieria y Economia del Transporte (Ineco)", "Spain"),
        ("Israel Discount Bank", "Israel"),
        ("Israel Railways Corporation", "Israel"),
        ("Italek", "Israel"),
        ("Kass - C", "Israel"),
        ("Kavim Public Transportation", "Israel"),
        ("Kfar Giladi Quarries Agricultural Cooperative Association", "Israel"),
        ("Kotler Adika Building Company", "Israel"),
        ("Lapidoth Capital", "Israel"),
        ("Marom Tuval Consulting Management & Investments", "Israel"),
        ("Matrix IT", "Israel"),
        ("Mayer's Cars and Trucks Co", "Israel"),
        ("Medan Roads and Quarries", "Israel"),
        ("Mega Or Holdings", "Israel"),
        ("Mekorot", "Israel"),
        ("Mercantile Discount Bank", "Israel"),
        ("MER Group", "Israel"),
        ("Merkavim Transportation Technologies", "Israel"),
        ("Mery Building Works Contracting Company", "Israel"),
        ("Metrontario Investments", "Israel"),
        ("Minrav Group", "Israel"),
        ("Mishab Housing Construction & Development Company", "Israel"),
        ("Mishkan Eliyahu Construction and Investment Company", "Israel"),
        ("Mivne Group", "Israel"),
        ("Mizrachi & Sons Investments Group", "Israel"),
        ("Mizrahi Tefahot", "Israel"),
        ("Modi'in Ezrachi Group", "Israel"),
        ("Mordechai Aviv Taasiot Beniyah 1973", "Israel"),
        ("Motorola Solutions", "United States"),
        ("Motorola Solutions Israel", "Israel"),
        ("Natoon Nof Yam Security", "Israel"),
        ("New Way Traffic", "Israel"),
        ("Ofertex Industries 1997", "Israel"),
        ("Olenik Transportation Earth Work and Road Constructions", "Israel"),
        ("Oron Group Investments & Holdings", "Israel"),
        ("Partner Communications", "Israel"),
        ("Pelephone", "Israel"),
        ("Powergen Solar A", "Israel"),
        ("Proffimat SR", "Israel"),
        ("Rami Levi Chain Stores Hashikma Marketing 2006", "Israel"),
        ("Rami Levy Hashikma Marketing Communication", "Israel"),
        ("Re/Max Holdings", "United States"),
        ("Rotshtein Real Estate", "Israel"),
        ("S.A.G. (Velvel) Building & Development", "Israel"),
        ("Salomon A Angel", "Israel"),
        ("Sarfati Shimon", "Israel"),
        ("Shalgal Foods", "Israel"),
        ("Shapir Engineering and Industry", "Israel"),
        ("Shlomo Cohen Construction Company", "Israel"),
        ("Shoham Engineering and Development", "Israel"),
        ("Shikun & Binui", "Israel"),
        ("Shufersal", "Israel"),
        ("Solel Boneh", "Israel"),
        ("Sonol Israel", "Israel"),
        ("Steconfer", "Portugal"),
        ("Superbus Transportation and Tourism", "Israel"),
        ("Supergum Industries 1969", "Israel"),
        ("Twitoplast", "Israel"),
        ("Unikowsky Maoz", "Israel"),
        ("Villar International", "Israel"),
        ("YAZ Construction and Development Company", "Israel"),
        ("YD Barazani", "Israel"),
        ("Yacobi Brothers Group", "Israel"),
        ("Yes TV and Communications Services", "Israel"),
        ("ZF Building Company", "Israel"),
        ("ZMH Hammerman", "Israel"),
        ("Zakai Agricultural Know-how and Inputs", "Israel"),
        ("Zriha Hlavin Industries", "Israel"),
    ]

    def fetch(self) -> bytes:
        # Data is hardcoded from OHCHR reports - no fetch needed
        return b""

    def parse(self, raw_data: bytes) -> list[dict]:
        entities = []

        for name, country in self.COMPANIES:
            entities.append({
                "source_id": name[:80],
                "entity_type": "company",
                "full_name": name,
                "first_name": "",
                "last_name": name,
                "aliases": [],
                "identifiers": [],
                "nationalities": [country] if country else [],
                "addresses": [country] if country else [],
                "birth_dates": [],
                "birth_places": [],
                "programs": ["OHCHR Settlement Database (HRC Res 31/36)"],
                "reasons": "Business enterprise involved in activities related to Israeli settlements in the Occupied Palestinian Territory",
                "legal_basis": "UN Human Rights Council Resolution 31/36",
                "source_url": "https://www.ohchr.org/en/hr-bodies/hrc/regular-sessions/session31/database-hrc3136",
            })

        return entities
