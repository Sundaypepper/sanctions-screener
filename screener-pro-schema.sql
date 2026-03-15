-- ============================================================
-- SCREENER PRO: Subscription-based supplier screening
-- Runs in the EXISTING sanctions-screener Supabase project
-- (aiweylrcdrcrksypfdov) alongside the entities table.
-- ============================================================

-- 1. PROFILES (extends auth.users)
CREATE TABLE IF NOT EXISTS profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  organisation_name TEXT NOT NULL DEFAULT '',
  contact_email TEXT NOT NULL DEFAULT '',
  notification_email TEXT,  -- separate email for alerts (optional)
  match_threshold REAL NOT NULL DEFAULT 0.5,
  notify_on_new_match BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Auto-create profile on signup
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO public.profiles (id, contact_email)
  VALUES (NEW.id, NEW.email);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION handle_new_user();


-- 2. SUPPLIER LISTS
CREATE TABLE IF NOT EXISTS supplier_lists (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  screening_frequency TEXT NOT NULL DEFAULT 'weekly'
    CHECK (screening_frequency IN ('weekly', 'monthly', 'manual')),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_screened_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_supplier_lists_user ON supplier_lists(user_id);


-- 3. SUPPLIERS
CREATE TABLE IF NOT EXISTS suppliers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  list_id UUID NOT NULL REFERENCES supplier_lists(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  country TEXT DEFAULT '',
  org_number TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_suppliers_list ON suppliers(list_id);
CREATE INDEX idx_suppliers_user ON suppliers(user_id);
CREATE INDEX idx_suppliers_name ON suppliers USING gin (name gin_trgm_ops);


-- 4. SCREENING RESULTS
CREATE TABLE IF NOT EXISTS screening_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  supplier_id UUID NOT NULL REFERENCES suppliers(id) ON DELETE CASCADE,
  run_id UUID,  -- references screening_runs(id), set later
  -- Match data from search_sanctions
  matched_entity_name TEXT NOT NULL,
  matched_source TEXT NOT NULL,
  match_score REAL NOT NULL,
  entity_type TEXT DEFAULT '',
  nationalities JSONB DEFAULT '[]',
  programs JSONB DEFAULT '[]',
  reasons TEXT DEFAULT '',
  source_url TEXT DEFAULT '',
  -- Status
  is_new BOOLEAN NOT NULL DEFAULT TRUE,
  reviewed BOOLEAN NOT NULL DEFAULT FALSE,
  reviewed_at TIMESTAMPTZ,
  reviewed_by TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  -- Timestamps
  found_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_screening_results_user ON screening_results(user_id);
CREATE INDEX idx_screening_results_supplier ON screening_results(supplier_id);
CREATE INDEX idx_screening_results_new ON screening_results(user_id, is_new) WHERE is_new = TRUE;


-- 5. SCREENING RUNS (audit log)
CREATE TABLE IF NOT EXISTS screening_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  list_id UUID REFERENCES supplier_lists(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'completed', 'failed')),
  suppliers_screened INTEGER DEFAULT 0,
  new_matches_found INTEGER DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  error_message TEXT
);

CREATE INDEX idx_screening_runs_user ON screening_runs(user_id);
CREATE INDEX idx_screening_runs_list ON screening_runs(list_id);

-- Add FK from screening_results to screening_runs
ALTER TABLE screening_results
  ADD CONSTRAINT fk_screening_results_run
  FOREIGN KEY (run_id) REFERENCES screening_runs(id) ON DELETE SET NULL;


-- 6. EMAIL LOG (track sent notifications)
CREATE TABLE IF NOT EXISTS email_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  run_id UUID REFERENCES screening_runs(id) ON DELETE SET NULL,
  email_to TEXT NOT NULL,
  subject TEXT NOT NULL,
  matches_count INTEGER DEFAULT 0,
  sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resend_id TEXT  -- Resend API message ID
);


-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================

ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE supplier_lists ENABLE ROW LEVEL SECURITY;
ALTER TABLE suppliers ENABLE ROW LEVEL SECURITY;
ALTER TABLE screening_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE screening_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_log ENABLE ROW LEVEL SECURITY;

-- Profiles: users can only see/edit their own
CREATE POLICY "Users can view own profile"
  ON profiles FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Users can update own profile"
  ON profiles FOR UPDATE USING (auth.uid() = id);

-- Supplier lists: users can CRUD their own
CREATE POLICY "Users can view own lists"
  ON supplier_lists FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can insert own lists"
  ON supplier_lists FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own lists"
  ON supplier_lists FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own lists"
  ON supplier_lists FOR DELETE USING (auth.uid() = user_id);

-- Suppliers: users can CRUD their own
CREATE POLICY "Users can view own suppliers"
  ON suppliers FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can insert own suppliers"
  ON suppliers FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own suppliers"
  ON suppliers FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own suppliers"
  ON suppliers FOR DELETE USING (auth.uid() = user_id);

-- Screening results: users can view/update their own
CREATE POLICY "Users can view own results"
  ON screening_results FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can update own results"
  ON screening_results FOR UPDATE USING (auth.uid() = user_id);

-- Screening runs: users can view their own
CREATE POLICY "Users can view own runs"
  ON screening_runs FOR SELECT USING (auth.uid() = user_id);

-- Email log: users can view their own
CREATE POLICY "Users can view own emails"
  ON email_log FOR SELECT USING (auth.uid() = user_id);


-- ============================================================
-- SCREENING FUNCTION
-- Screens all suppliers in a list against entities table
-- Returns new matches found
-- ============================================================

CREATE OR REPLACE FUNCTION screen_supplier_list(
  p_list_id UUID,
  p_threshold REAL DEFAULT 0.5,
  p_max_per_supplier INTEGER DEFAULT 5
)
RETURNS TABLE (
  supplier_id UUID,
  supplier_name TEXT,
  matched_entity TEXT,
  matched_source TEXT,
  score REAL,
  entity_type TEXT,
  programs JSONB,
  reasons TEXT,
  source_url TEXT,
  nationalities JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_user_id UUID;
  v_run_id UUID;
  v_supplier RECORD;
  v_match RECORD;
  v_screened INTEGER := 0;
  v_new_matches INTEGER := 0;
BEGIN
  -- Get the list owner
  SELECT sl.user_id INTO v_user_id
  FROM supplier_lists sl WHERE sl.id = p_list_id;

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'List not found';
  END IF;

  -- Create a screening run
  INSERT INTO screening_runs (user_id, list_id, status)
  VALUES (v_user_id, p_list_id, 'running')
  RETURNING id INTO v_run_id;

  -- Loop through each supplier in the list
  FOR v_supplier IN
    SELECT s.id, s.name FROM suppliers s WHERE s.list_id = p_list_id
  LOOP
    v_screened := v_screened + 1;

    -- Search for matches using pg_trgm similarity
    FOR v_match IN
      SELECT
        e.full_name,
        e.source,
        similarity(lower(v_supplier.name), lower(e.full_name)) AS sim_score,
        e.entity_type,
        e.programs,
        e.reasons,
        e.source_url,
        e.nationalities
      FROM entities e
      WHERE lower(v_supplier.name) % lower(e.full_name)
        AND similarity(lower(v_supplier.name), lower(e.full_name)) >= p_threshold
      ORDER BY sim_score DESC
      LIMIT p_max_per_supplier
    LOOP
      -- Check if this exact match already exists (avoid duplicates)
      IF NOT EXISTS (
        SELECT 1 FROM screening_results sr
        WHERE sr.supplier_id = v_supplier.id
          AND sr.matched_entity_name = v_match.full_name
          AND sr.matched_source = v_match.source
      ) THEN
        -- Insert new match
        INSERT INTO screening_results (
          user_id, supplier_id, run_id,
          matched_entity_name, matched_source, match_score,
          entity_type, nationalities, programs, reasons, source_url,
          is_new
        ) VALUES (
          v_user_id, v_supplier.id, v_run_id,
          v_match.full_name, v_match.source, v_match.sim_score,
          v_match.entity_type, v_match.nationalities, v_match.programs,
          v_match.reasons, v_match.source_url,
          TRUE
        );
        v_new_matches := v_new_matches + 1;
      END IF;

      -- Return the match row
      supplier_id := v_supplier.id;
      supplier_name := v_supplier.name;
      matched_entity := v_match.full_name;
      matched_source := v_match.source;
      score := v_match.sim_score;
      entity_type := v_match.entity_type;
      programs := v_match.programs;
      reasons := v_match.reasons;
      source_url := v_match.source_url;
      nationalities := v_match.nationalities;
      RETURN NEXT;
    END LOOP;
  END LOOP;

  -- Update run status
  UPDATE screening_runs
  SET status = 'completed',
      suppliers_screened = v_screened,
      new_matches_found = v_new_matches,
      completed_at = NOW()
  WHERE id = v_run_id;

  -- Update list last_screened_at
  UPDATE supplier_lists
  SET last_screened_at = NOW(), updated_at = NOW()
  WHERE id = p_list_id;

  RETURN;
END;
$$;

-- Grant execute to authenticated users
GRANT EXECUTE ON FUNCTION screen_supplier_list TO authenticated;


-- ============================================================
-- HELPER: Get new (unreviewed) matches for a user
-- ============================================================
CREATE OR REPLACE FUNCTION get_new_matches(p_user_id UUID DEFAULT NULL)
RETURNS TABLE (
  result_id UUID,
  supplier_name TEXT,
  list_name TEXT,
  matched_entity_name TEXT,
  matched_source TEXT,
  match_score REAL,
  entity_type TEXT,
  programs JSONB,
  reasons TEXT,
  source_url TEXT,
  found_at TIMESTAMPTZ
)
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT
    sr.id AS result_id,
    s.name AS supplier_name,
    sl.name AS list_name,
    sr.matched_entity_name,
    sr.matched_source,
    sr.match_score,
    sr.entity_type,
    sr.programs,
    sr.reasons,
    sr.source_url,
    sr.found_at
  FROM screening_results sr
  JOIN suppliers s ON s.id = sr.supplier_id
  JOIN supplier_lists sl ON sl.id = s.list_id
  WHERE sr.user_id = COALESCE(p_user_id, auth.uid())
    AND sr.is_new = TRUE
  ORDER BY sr.match_score DESC, sr.found_at DESC;
$$;

GRANT EXECUTE ON FUNCTION get_new_matches TO authenticated;


-- ============================================================
-- HELPER: Mark matches as reviewed
-- ============================================================
CREATE OR REPLACE FUNCTION mark_matches_reviewed(p_result_ids UUID[])
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_count INTEGER;
BEGIN
  UPDATE screening_results
  SET is_new = FALSE,
      reviewed = TRUE,
      reviewed_at = NOW()
  WHERE id = ANY(p_result_ids)
    AND user_id = auth.uid();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION mark_matches_reviewed TO authenticated;


-- ============================================================
-- SERVICE ROLE FUNCTION: Screen all active lists (for cron job)
-- Only callable by service_role, not anon/authenticated
-- ============================================================
CREATE OR REPLACE FUNCTION screen_all_active_lists()
RETURNS TABLE (
  list_id UUID,
  list_name TEXT,
  user_email TEXT,
  new_matches INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_list RECORD;
  v_match_count INTEGER;
BEGIN
  FOR v_list IN
    SELECT sl.id, sl.name, sl.user_id, p.contact_email,
           COALESCE(p.notification_email, p.contact_email) AS notify_email,
           p.match_threshold
    FROM supplier_lists sl
    JOIN profiles p ON p.id = sl.user_id
    WHERE sl.is_active = TRUE
      AND sl.screening_frequency IN ('weekly', 'monthly')
      AND (
        -- Weekly: screen if not screened in last 6 days
        (sl.screening_frequency = 'weekly' AND
         (sl.last_screened_at IS NULL OR sl.last_screened_at < NOW() - INTERVAL '6 days'))
        OR
        -- Monthly: screen if not screened in last 27 days
        (sl.screening_frequency = 'monthly' AND
         (sl.last_screened_at IS NULL OR sl.last_screened_at < NOW() - INTERVAL '27 days'))
      )
  LOOP
    -- Screen the list
    PERFORM screen_supplier_list(v_list.id, v_list.match_threshold);

    -- Count new matches from this run
    SELECT COUNT(*) INTO v_match_count
    FROM screening_results sr
    JOIN suppliers s ON s.id = sr.supplier_id
    WHERE s.list_id = v_list.id
      AND sr.is_new = TRUE;

    list_id := v_list.id;
    list_name := v_list.name;
    user_email := v_list.notify_email;
    new_matches := v_match_count;
    RETURN NEXT;
  END LOOP;
END;
$$;

-- Only service_role can call this (for the cron webhook)
REVOKE EXECUTE ON FUNCTION screen_all_active_lists FROM anon, authenticated;
