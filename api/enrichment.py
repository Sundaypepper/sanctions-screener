"""
Maps sanctions program codes to human-readable descriptions
and Norwegian procurement law (FOA) references.
"""

# OFAC program codes → description + FOA relevance
OFAC_PROGRAMS = {
    # Russia/Ukraine
    "RUSSIA-EO14024": {
        "desc": "Russia-related sanctions (Executive Order 14024)",
        "desc_no": "Russland-sanksjoner – tiltak mot personer/selskaper knyttet til Russlands regjering og forsvarsindustri",
        "category": "sanctions",
    },
    "UKRAINE-EO13662": {
        "desc": "Ukraine-related sectoral sanctions",
        "desc_no": "Ukraina-relaterte sektorsanksjoner – restriksjoner på russisk finans, energi og forsvarsteknologi",
        "category": "sanctions",
    },
    "UKRAINE-EO13661": {
        "desc": "Ukraine-related sanctions targeting individuals",
        "desc_no": "Ukraina-relaterte sanksjoner mot enkeltpersoner som undergraver Ukrainas suverenitet",
        "category": "sanctions",
    },
    
    # Terrorism
    "SDGT": {
        "desc": "Specially Designated Global Terrorist",
        "desc_no": "Globalt utpekt terrorist – person/organisasjon knyttet til terrorvirksomhet",
        "category": "terrorism",
        "foa_ref": "FOA § 24-2 (1) bokstav d: terrorhandlinger eller finansiering av terrorisme",
    },
    "SDNTK": {
        "desc": "Specially Designated Narcotics Trafficker Kingpin",
        "desc_no": "Utpekt narkotikasmugler – sentral aktør i internasjonal narkotikahandel",
        "category": "narcotics",
        "foa_ref": "FOA § 24-2 (1) bokstav a: deltakelse i en kriminell organisasjon",
    },
    "TCO": {
        "desc": "Transnational Criminal Organization",
        "desc_no": "Transnasjonal kriminell organisasjon",
        "category": "crime",
        "foa_ref": "FOA § 24-2 (1) bokstav a: deltakelse i en kriminell organisasjon",
    },
    "SDNT": {
        "desc": "Specially Designated Narcotics Trafficker",
        "desc_no": "Utpekt narkotikasmugler",
        "category": "narcotics",
    },
    
    # Iran
    "IFSR": {
        "desc": "Iranian Financial Sanctions Regulations",
        "desc_no": "Iranske finanssanksjoner – tiltak mot Irans kjernefysiske program og finanssektor",
        "category": "sanctions",
    },
    "IRAN": {
        "desc": "Iran sanctions program",
        "desc_no": "Iran-sanksjoner – generelle restriksjoner",
        "category": "sanctions",
    },
    "IRAN-EO13902": {
        "desc": "Iran sanctions (Executive Order 13902)",
        "desc_no": "Iran-sanksjoner – restriksjoner på bestemte sektorer",
        "category": "sanctions",
    },
    "IRAN-EO13846": {
        "desc": "Iran sanctions (Executive Order 13846)",
        "desc_no": "Iran-sanksjoner – sekundærsanksjoner mot enheter som handler med Iran",
        "category": "sanctions",
    },
    "IRAN-HR": {
        "desc": "Iran Human Rights sanctions",
        "desc_no": "Iran-sanksjoner – menneskerettighetsbrudd",
        "category": "human_rights",
    },
    "IRGC": {
        "desc": "Islamic Revolutionary Guard Corps",
        "desc_no": "Irans revolusjonsgarde (IRGC) – militær organisasjon",
        "category": "terrorism",
    },
    
    # WMD / Nonproliferation
    "NPWMD": {
        "desc": "Nonproliferation of Weapons of Mass Destruction",
        "desc_no": "Spredning av masseødeleggelsesvåpen – person/selskap involvert i WMD-programmer",
        "category": "wmd",
    },
    "DPRK3": {
        "desc": "North Korea sanctions (DPRK)",
        "desc_no": "Nord-Korea-sanksjoner – kjernefysisk program og menneskerettighetsbrudd",
        "category": "sanctions",
    },
    "DPRK4": {
        "desc": "North Korea sanctions (DPRK) - expanded",
        "desc_no": "Nord-Korea-sanksjoner – utvidet",
        "category": "sanctions",
    },
    
    # Human rights / Magnitsky
    "GLOMAG": {
        "desc": "Global Magnitsky Human Rights sanctions",
        "desc_no": "Magnitsky-sanksjoner – alvorlige menneskerettighetsbrudd og korrupsjon globalt",
        "category": "human_rights",
        "foa_ref": "FOA § 24-2 (1) bokstav e: barnearbeid og andre former for menneskehandel (ved relevante forhold)",
    },
    
    # Drugs
    "ILLICIT-DRUGS-EO14059": {
        "desc": "Illicit drug trafficking sanctions",
        "desc_no": "Ulovlig narkotikahandel – sanksjoner mot sentrale aktører",
        "category": "narcotics",
    },
    
    # Cyber
    "CYBER2": {
        "desc": "Cyber-related sanctions",
        "desc_no": "Cybersanksjoner – ondsinnet cyberaktivitet mot USA/allierte",
        "category": "cyber",
    },
    
    # Country-specific
    "CUBA": {
        "desc": "Cuba sanctions",
        "desc_no": "Cuba-sanksjoner",
        "category": "sanctions",
    },
    "VENEZUELA": {
        "desc": "Venezuela sanctions",
        "desc_no": "Venezuela-sanksjoner",
        "category": "sanctions",
    },
    "VENEZUELA-EO13850": {
        "desc": "Venezuela sanctions (Executive Order 13850)",
        "desc_no": "Venezuela-sanksjoner – gull- og oljesektoren",
        "category": "sanctions",
    },
    "BELARUS-EO14038": {
        "desc": "Belarus sanctions",
        "desc_no": "Hviterussland-sanksjoner – Lukashenko-regimet",
        "category": "sanctions",
    },
    "BALKANS": {
        "desc": "Western Balkans sanctions",
        "desc_no": "Balkan-sanksjoner – destabilisering av regionen",
        "category": "sanctions",
    },
    "IRAQ2": {
        "desc": "Iraq-related sanctions",
        "desc_no": "Irak-sanksjoner",
        "category": "sanctions",
    },
    "PAARSSR-EO13894": {
        "desc": "Syria sanctions (Protecting Against Authoritarian Regime's Sanctions)",
        "desc_no": "Syria-sanksjoner – Assad-regimet og tilknyttede aktører",
        "category": "sanctions",
    },
}

# Source-level descriptions
SOURCE_DESCRIPTIONS = {
    "ofac_sdn": {
        "name": "US OFAC SDN",
        "desc_no": "USAs sanksjonsliste – personer og selskaper som er blokkert av det amerikanske finansdepartementet",
        "authority": "U.S. Department of the Treasury, Office of Foreign Assets Control",
    },
    "un_sc": {
        "name": "FNs Sikkerhetsråd",
        "desc_no": "FN-sanksjoner vedtatt av Sikkerhetsrådet – bindende for alle medlemsland inkludert Norge",
        "authority": "United Nations Security Council",
        "foa_ref": "Direkte relevant for FOA § 24-2 – Norge er folkerettslig forpliktet til å implementere FN-sanksjoner",
    },
    "eu_fsf": {
        "name": "EU Financial Sanctions",
        "desc_no": "EUs finansielle sanksjonsliste – Norge implementerer gjennom EØS-avtalen og egne forskrifter",
        "authority": "European Commission, DG FISMA",
        "foa_ref": "Direkte relevant for FOA § 24-2 – Norge implementerer EU-sanksjoner",
    },
    "uk_ofsi": {
        "name": "UK Sanctions",
        "desc_no": "Storbritannias sanksjonsliste – selvstendig etter Brexit, overlapper med EU",
        "authority": "HM Treasury, Office of Financial Sanctions Implementation",
    },
    "interpol": {
        "name": "Interpol Red Notices",
        "desc_no": "Internasjonale etterlysninger – personer som er etterlyst for pågripelse med henblikk på utlevering",
        "authority": "INTERPOL",
        "foa_ref": "Relevant for FOA § 24-2 – etterlyst for straffbare forhold som kan gi avvisningsplikt",
    },
    "worldbank": {
        "name": "Verdensbanken Debarment",
        "desc_no": "Leverandører utestengt fra verdensbank-finansierte kontrakter pga. bedrageri, korrupsjon eller samarbeid om prismanipulasjon",
        "authority": "World Bank Group, Integrity Vice Presidency",
        "foa_ref": "FOA § 24-5 (1) bokstav d: utestengt fra deltakelse i offentlige kontrakter av en domstol eller et forvaltningsorgan",
    },
    "canada_sema": {
        "name": "Canada Sanctions",
        "desc_no": "Canadas autonome sanksjonsliste",
        "authority": "Global Affairs Canada",
    },
    "australia_dfat": {
        "name": "Australia DFAT Sanctions",
        "desc_no": "Australias konsoliderte sanksjonsliste",
        "authority": "Department of Foreign Affairs and Trade",
    },
    "swiss_seco": {
        "name": "Sveits SECO",
        "desc_no": "Sveitsiske sanksjoner og embargoer",
        "authority": "State Secretariat for Economic Affairs (SECO)",
    },
    "us_bis_denied": {
        "name": "US BIS Denied Persons",
        "desc_no": "Personer/selskaper nektet eksportprivilegier fra USA – brudd på eksportkontrollregler",
        "authority": "Bureau of Industry and Security, US Department of Commerce",
    },
    "us_sam": {
        "name": "US SAM Exclusions",
        "desc_no": "Leverandører utestengt fra amerikanske føderale kontrakter",
        "authority": "General Services Administration (GSA)",
        "foa_ref": "FOA § 24-5 (1) bokstav d: utestengt fra deltakelse i offentlige kontrakter",
    },
}

# FOA categories mapping
FOA_CATEGORIES = {
    "terrorism": {
        "foa_ref": "FOA § 24-2 (1) bokstav d",
        "obligation": "OBLIGATORISK AVVISNING",
        "desc_no": "Terrorhandlinger eller finansiering av terrorisme",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (1) bokstav d",
    },
    "crime": {
        "foa_ref": "FOA § 24-2 (1) bokstav a",
        "obligation": "OBLIGATORISK AVVISNING",
        "desc_no": "Deltakelse i en kriminell organisasjon",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (1) bokstav a",
    },
    "narcotics": {
        "foa_ref": "FOA § 24-2 (1) bokstav a/c",
        "obligation": "OBLIGATORISK AVVISNING",
        "desc_no": "Organisert kriminalitet / hvitvasking av penger",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (1) bokstav a/c",
    },
    "human_rights": {
        "foa_ref": "FOA § 24-2 (1) bokstav e / § 24-5",
        "obligation": "OBLIGATORISK eller FRIVILLIG AVVISNING",
        "desc_no": "Menneskehandel/barnearbeid (obligatorisk) eller andre alvorlige forhold (frivillig)",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (1) bokstav f / (4)",
    },
    "sanctions": {
        "foa_ref": "FOA § 24-5 (1)",
        "obligation": "FRIVILLIG AVVISNING",
        "desc_no": "Oppdragsgiver kan avvise leverandøren dersom den er oppført på internasjonal sanksjonsliste",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (4)",
    },
    "wmd": {
        "foa_ref": "FOA § 24-5 (1)",
        "obligation": "FRIVILLIG AVVISNING",
        "desc_no": "Spredning av masseødeleggelsesvåpen",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (4)",
    },
    "cyber": {
        "foa_ref": "FOA § 24-5 (1)",
        "obligation": "FRIVILLIG AVVISNING",
        "desc_no": "Ondsinnet cyberaktivitet",
        "directive_ref": "Direktiv 2014/24/EU art. 57 (4)",
    },
}


def enrich_result(entity: dict) -> dict:
    """
    Enrich a search result with human-readable reasons and FOA references.
    """
    source = entity.get('source', '')
    programs = entity.get('programs', [])
    
    # Source info
    source_info = SOURCE_DESCRIPTIONS.get(source, {})
    entity['source_name'] = source_info.get('name', source)
    entity['source_description'] = source_info.get('desc_no', '')
    entity['source_authority'] = source_info.get('authority', '')
    
    # Program descriptions and FOA mapping
    program_details = []
    foa_references = set()
    obligation_level = 'FRIVILLIG AVVISNING'  # Default
    categories = set()
    
    for prog in programs:
        prog_info = OFAC_PROGRAMS.get(prog, {})
        
        detail = {
            'code': prog,
            'description': prog_info.get('desc_no', prog),
            'description_en': prog_info.get('desc', prog),
            'category': prog_info.get('category', 'sanctions'),
        }
        program_details.append(detail)
        
        cat = prog_info.get('category', 'sanctions')
        categories.add(cat)
        
        # Get FOA reference from program or category
        if 'foa_ref' in prog_info:
            foa_references.add(prog_info['foa_ref'])
        
        cat_info = FOA_CATEGORIES.get(cat, {})
        if cat_info:
            foa_references.add(cat_info.get('foa_ref', ''))
            if cat_info.get('obligation') == 'OBLIGATORISK AVVISNING':
                obligation_level = 'OBLIGATORISK AVVISNING'
    
    # Source-level FOA ref
    if 'foa_ref' in source_info:
        foa_references.add(source_info['foa_ref'])
    
    # Special handling for specific sources
    if source == 'worldbank':
        obligation_level = 'FRIVILLIG AVVISNING'
        foa_references.add('FOA § 24-5 (1) bokstav d: utestengt fra deltakelse i offentlige kontrakter')
        entity['reason_no'] = 'Utestengt fra verdensbank-finansierte kontrakter grunnet bedrageri, korrupsjon eller lignende forhold'
    
    if source == 'interpol':
        obligation_level = 'VURDER OBLIGATORISK AVVISNING'
        foa_references.add('FOA § 24-2: Avhenger av det straffbare forholdet personen er etterlyst for')
        entity['reason_no'] = 'Etterlyst av Interpol – det underliggende straffbare forholdet avgjør om avvisningsplikten i § 24-2 er aktuell'
    
    # Build summary
    if not entity.get('reason_no'):
        category_descs = []
        for cat in categories:
            cat_info = FOA_CATEGORIES.get(cat, {})
            if cat_info.get('desc_no'):
                category_descs.append(cat_info['desc_no'])
        entity['reason_no'] = '; '.join(category_descs) if category_descs else 'Oppført på internasjonal sanksjonsliste'
    
    entity['program_details'] = program_details
    entity['foa_references'] = sorted([r for r in foa_references if r])
    entity['obligation_level'] = obligation_level
    entity['categories'] = sorted(categories)
    
    # Self-cleaning note
    entity['self_cleaning_note'] = (
        'Leverandøren kan unngå avvisning ved å dokumentere self-cleaning etter FOA § 24-6: '
        '(1) betalt erstatning, (2) samarbeidet med myndighetene, og '
        '(3) iverksatt tiltak for å hindre gjentakelse.'
    )
    
    return entity
