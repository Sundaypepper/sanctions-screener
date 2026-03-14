-- ============================================
-- SANKSJONSSCREENER – SUPABASE OPPSETT
-- ============================================
-- Kjør dette i Supabase SQL Editor for nytt prosjekt.
-- Krever pg_trgm extension (aktivert som standard i Supabase).

-- 1. Aktiver extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- 2. Hovedtabell for sanksjonsdata
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,                    -- "{source}:{source_id}"
    source TEXT NOT NULL,
    source_id TEXT,
    entity_type TEXT DEFAULT 'unknown',     -- person, company, vessel, aircraft

    -- Navn
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    aliases JSONB DEFAULT '[]'::jsonb,
    name_original_script TEXT,

    -- Identifikatorer
    identifiers JSONB DEFAULT '[]'::jsonb,  -- [{type, value}]

    -- Detaljer
    nationalities JSONB DEFAULT '[]'::jsonb,
    addresses JSONB DEFAULT '[]'::jsonb,
    birth_dates JSONB DEFAULT '[]'::jsonb,
    birth_places JSONB DEFAULT '[]'::jsonb,

    -- Sanksjonsinfo
    listed_date TEXT,
    delisted_date TEXT,
    programs JSONB DEFAULT '[]'::jsonb,
    reasons TEXT,
    legal_basis TEXT,
    source_url TEXT,

    -- Metadata
    raw_data JSONB,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_updated TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(source, source_id)
);

-- 3. Indekser for ytelse
CREATE INDEX IF NOT EXISTS idx_entities_source ON entities(source);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_listed ON entities(listed_date);

-- Trigram-indekser for fuzzy søk (GIN er raskest for pg_trgm)
CREATE INDEX IF NOT EXISTS idx_entities_name_trgm ON entities USING gin (full_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entities_first_trgm ON entities USING gin (first_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entities_last_trgm ON entities USING gin (last_name gin_trgm_ops);

-- Tekstsøk-indeks
CREATE INDEX IF NOT EXISTS idx_entities_name_text ON entities USING gin (to_tsvector('simple', coalesce(full_name, '')));

-- 4. Kildestatustabell
CREATE TABLE IF NOT EXISTS source_status (
    source TEXT PRIMARY KEY,
    last_sync TIMESTAMPTZ,
    last_success TIMESTAMPTZ,
    entity_count INTEGER DEFAULT 0,
    error_message TEXT,
    sync_duration_seconds REAL
);

-- 5. Søkefunksjon med fuzzy matching
-- Kaller denne fra frontend via supabase.rpc('search_sanctions', { ... })
CREATE OR REPLACE FUNCTION search_sanctions(
    search_query TEXT,
    match_threshold REAL DEFAULT 0.3,
    max_results INTEGER DEFAULT 20,
    filter_type TEXT DEFAULT NULL,
    filter_sources TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    id TEXT,
    source TEXT,
    source_id TEXT,
    entity_type TEXT,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    aliases JSONB,
    identifiers JSONB,
    nationalities JSONB,
    addresses JSONB,
    birth_dates JSONB,
    birth_places JSONB,
    listed_date TEXT,
    programs JSONB,
    reasons TEXT,
    legal_basis TEXT,
    source_url TEXT,
    score REAL
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    normalized_query TEXT;
BEGIN
    -- Normaliser søket
    normalized_query := lower(unaccent(trim(search_query)));

    -- Sett threshold for pg_trgm
    PERFORM set_limit(match_threshold);

    RETURN QUERY
    SELECT
        e.id,
        e.source,
        e.source_id,
        e.entity_type,
        e.full_name,
        e.first_name,
        e.last_name,
        e.aliases,
        e.identifiers,
        e.nationalities,
        e.addresses,
        e.birth_dates,
        e.birth_places,
        e.listed_date,
        e.programs,
        e.reasons,
        e.legal_basis,
        e.source_url,
        GREATEST(
            similarity(lower(unaccent(coalesce(e.full_name, ''))), normalized_query),
            similarity(lower(unaccent(coalesce(e.first_name || ' ' || e.last_name, ''))), normalized_query)
        )::REAL AS score
    FROM entities e
    WHERE
        -- Trigram similarity filter
        (
            lower(unaccent(coalesce(e.full_name, ''))) % normalized_query
            OR lower(unaccent(coalesce(e.first_name || ' ' || e.last_name, ''))) % normalized_query
        )
        -- Optional type filter
        AND (filter_type IS NULL OR e.entity_type = filter_type)
        -- Optional source filter
        AND (filter_sources IS NULL OR e.source = ANY(filter_sources))
    ORDER BY score DESC
    LIMIT max_results;
END;
$$;

-- 6. Identifikator-søk
CREATE OR REPLACE FUNCTION search_by_identifier(
    id_type_param TEXT,
    id_value_param TEXT
)
RETURNS TABLE (
    id TEXT,
    source TEXT,
    full_name TEXT,
    entity_type TEXT,
    identifiers JSONB,
    nationalities JSONB,
    listed_date TEXT,
    programs JSONB,
    reasons TEXT,
    source_url TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.id,
        e.source,
        e.full_name,
        e.entity_type,
        e.identifiers,
        e.nationalities,
        e.listed_date,
        e.programs,
        e.reasons,
        e.source_url
    FROM entities e
    WHERE e.identifiers::text ILIKE '%' || id_value_param || '%'
    LIMIT 20;
END;
$$;

-- 7. Row Level Security – åpent lesetilgang, kun service_role kan skrive
ALTER TABLE entities ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_status ENABLE ROW LEVEL SECURITY;

-- Alle kan lese (anon + authenticated)
CREATE POLICY "Alle kan lese entities" ON entities
    FOR SELECT USING (true);

CREATE POLICY "Alle kan lese source_status" ON source_status
    FOR SELECT USING (true);

-- Kun service_role (crawlerne) kan skrive
CREATE POLICY "Service role kan skrive entities" ON entities
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service role kan skrive source_status" ON source_status
    FOR ALL USING (auth.role() = 'service_role');

-- 8. Gi anon-rollen tilgang til RPC-funksjonene
GRANT EXECUTE ON FUNCTION search_sanctions TO anon;
GRANT EXECUTE ON FUNCTION search_sanctions TO authenticated;
GRANT EXECUTE ON FUNCTION search_by_identifier TO anon;
GRANT EXECUTE ON FUNCTION search_by_identifier TO authenticated;
