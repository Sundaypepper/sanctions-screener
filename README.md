# Sanksjonsscreener – Leverandørscreening for offentlige anskaffelser

Åpen sanksjonsscreener som henter data direkte fra offentlige primærkilder.
Ingen lisenskostnader – alle datakilder er gratis og offentlig tilgjengelige.

## Datakilder (11 stk)

| Kilde | Format | Oppdatering | Entiteter |
|---|---|---|---|
| US OFAC SDN | XML | Daglig | ~17 000 |
| FN Sikkerhetsråd | XML | Ved endring | ~1 000 |
| EU Financial Sanctions (FSF) | XML | Daglig | ~2 500 |
| UK OFSI | CSV | Daglig | ~3 500 |
| Sveits SECO | XML | Daglig | ~3 000 |
| Canada SEMA | XML | Daglig | ~2 500 |
| Australia DFAT | CSV | Daglig | ~1 000 |
| Interpol Red Notices | JSON API | Live | ~7 000 |
| Verdensbanken Debarment | HTML/JSON | Hver 3. time | ~1 500 |
| US SAM Exclusions | API | Daglig | ~100 000 |
| US BIS Denied Persons | CSV | Daglig | ~700 |

**Total dekning: ~140 000+ entiteter**

## Kjøre

```bash
# Start med Docker
docker compose up -d

# Kjør initial sync
python -m crawlers.run_all

# API tilgjengelig på
http://localhost:8000/docs
```

## API-endepunkter

- `GET /api/screen?q=<navn>&threshold=0.7` – Søk person/selskap
- `GET /api/screen?org_nr=<nr>` – Søk på org.nr (kobler mot Enhetsregisteret)
- `POST /api/batch` – Batch-screening (JSON/CSV)
- `GET /api/sources` – Status per kilde
- `GET /api/entity/{id}` – Full detalj

## Anskaffelsesrettslig kontekst

- **FOA § 24-2**: Obligatorisk avvisning (terror, hvitvasking, bedrageri, korrupsjon)
- **FOA § 24-5**: Kan avvise (alvorlig yrkesmessig feil, utestengt, sanksjonslistet)
- **Direktiv 2014/24/EU art. 57**: Tilsvarende EU-hjemmel
