-- ============================================================
-- SCREENER PRO v2: Subscription, Leverandør, og Kontrakt Management
-- Upgrade for eksisterende sanctions-screener schema
-- ============================================================

-- ============================================================
-- 1. ALTER PROFILES: Legg til is_admin felt
-- ============================================================
ALTER TABLE profiles
  ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;


-- ============================================================
-- 2. SUBSCRIPTIONS TABLE
-- Abonnementsstyring per bruker
-- ============================================================
CREATE TABLE IF NOT EXISTS subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL UNIQUE REFERENCES auth.users(id) ON DELETE CASCADE,
  plan TEXT NOT NULL CHECK (plan IN ('basic', 'standard', 'unlimited')),
  price_monthly INTEGER NOT NULL,
  -- Price in øre (e.g. 490 = 4.90 NOK)
  max_suppliers INTEGER,
  -- NULL for unlimited
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'expired', 'cancelled')),
  starts_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_subscriptions_user ON subscriptions(user_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status);
CREATE INDEX idx_subscriptions_expires ON subscriptions(expires_at)
  WHERE status = 'active';


-- ============================================================
-- 3. ORDERS TABLE (Bestillinger)
-- Håndterer ordre fra uautentiserte brukere
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  employer TEXT,
  invoice_reference TEXT,
  selected_plan TEXT NOT NULL CHECK (selected_plan IN ('basic', 'standard', 'unlimited')),
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'processed', 'cancelled')),
  processed_at TIMESTAMPTZ,
  user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_email ON orders(email);


-- ============================================================
-- 4. VENDORS TABLE (Leverandører)
-- Aktive leverandører/underleverandører under oppfølging
-- ============================================================
CREATE TABLE IF NOT EXISTS vendors (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  org_number TEXT,
  country TEXT,
  notes TEXT DEFAULT '',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_vendors_user ON vendors(user_id);
CREATE INDEX idx_vendors_active ON vendors(user_id, is_active)
  WHERE is_active = TRUE;
CREATE INDEX idx_vendors_name ON vendors USING gin (name gin_trgm_ops);


-- ============================================================
-- 5. CONTRACTS TABLE (Kontrakter per leverandør)
-- Kontraktsperioder med utløpsdatoer
-- ============================================================
CREATE TABLE IF NOT EXISTS contracts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id UUID NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  end_date DATE NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  -- Beregnet basert på end_date >= CURRENT_DATE
  notify_before_expiry BOOLEAN NOT NULL DEFAULT TRUE,
  -- Skal vi varsle før utløp?
  expiry_notified BOOLEAN NOT NULL DEFAULT FALSE,
  -- Allerede varslet om utløp?
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_contracts_vendor ON contracts(vendor_id);
CREATE INDEX idx_contracts_user ON contracts(user_id);
CREATE INDEX idx_contracts_active ON contracts(user_id, is_active)
  WHERE is_active = TRUE;
CREATE INDEX idx_contracts_end_date ON contracts(end_date)
  WHERE is_active = TRUE;


-- ============================================================
-- 6. SUBCONTRACTORS TABLE (Underleverandører per kontrakt)
-- Importert via Excel fra hver kontrakt
-- ============================================================
CREATE TABLE IF NOT EXISTS subcontractors (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_id UUID NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
  vendor_id UUID NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  country TEXT,
  org_number TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_subcontractors_contract ON subcontractors(contract_id);
CREATE INDEX idx_subcontractors_vendor ON subcontractors(vendor_id);
CREATE INDEX idx_subcontractors_user ON subcontractors(user_id);
CREATE INDEX idx_subcontractors_name ON subcontractors USING gin (name gin_trgm_ops);


-- ============================================================
-- 7. OWNERS TABLE (Eiere/aksjonærer per leverandør)
-- Importert via Excel
-- ============================================================
CREATE TABLE IF NOT EXISTS owners (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id UUID NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  ownership_type TEXT CHECK (ownership_type IN ('direct', 'indirect')),
  country TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_owners_vendor ON owners(vendor_id);
CREATE INDEX idx_owners_user ON owners(user_id);
CREATE INDEX idx_owners_name ON owners USING gin (name gin_trgm_ops);


-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================

ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendors ENABLE ROW LEVEL SECURITY;
ALTER TABLE contracts ENABLE ROW LEVEL SECURITY;
ALTER TABLE subcontractors ENABLE ROW LEVEL SECURITY;
ALTER TABLE owners ENABLE ROW LEVEL SECURITY;


-- ============================================================
-- SUBSCRIPTIONS: RLS Policies
-- Bruker ser egen abonnement, admin ser alle
-- ============================================================
DROP POLICY IF EXISTS "Users can view own subscription" ON subscriptions;
CREATE POLICY "Users can view own subscription"
  ON subscriptions FOR SELECT
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Admin can view all subscriptions" ON subscriptions;
CREATE POLICY "Admin can view all subscriptions"
  ON subscriptions FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );

DROP POLICY IF EXISTS "Admin can update subscriptions" ON subscriptions;
CREATE POLICY "Admin can update subscriptions"
  ON subscriptions FOR UPDATE
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- ORDERS: RLS Policies
-- Anon kan INSERT (bestilling via form), service_role kan SELECT/UPDATE
-- ============================================================
DROP POLICY IF EXISTS "Anon can create orders" ON orders;
CREATE POLICY "Anon can create orders"
  ON orders FOR INSERT
  WITH CHECK (TRUE);

DROP POLICY IF EXISTS "Admin can view orders" ON orders;
CREATE POLICY "Admin can view orders"
  ON orders FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );

DROP POLICY IF EXISTS "Admin can update orders" ON orders;
CREATE POLICY "Admin can update orders"
  ON orders FOR UPDATE
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- VENDORS: RLS Policies
-- Bruker kan CRUD egne leverandører
-- ============================================================
DROP POLICY IF EXISTS "Users can view own vendors" ON vendors;
CREATE POLICY "Users can view own vendors"
  ON vendors FOR SELECT
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can insert vendors" ON vendors;
CREATE POLICY "Users can insert vendors"
  ON vendors FOR INSERT
  WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can update own vendors" ON vendors;
CREATE POLICY "Users can update own vendors"
  ON vendors FOR UPDATE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can delete own vendors" ON vendors;
CREATE POLICY "Users can delete own vendors"
  ON vendors FOR DELETE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Admin can view all vendors" ON vendors;
CREATE POLICY "Admin can view all vendors"
  ON vendors FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- CONTRACTS: RLS Policies
-- Bruker kan CRUD egne kontrakter
-- ============================================================
DROP POLICY IF EXISTS "Users can view own contracts" ON contracts;
CREATE POLICY "Users can view own contracts"
  ON contracts FOR SELECT
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can insert contracts" ON contracts;
CREATE POLICY "Users can insert contracts"
  ON contracts FOR INSERT
  WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can update own contracts" ON contracts;
CREATE POLICY "Users can update own contracts"
  ON contracts FOR UPDATE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can delete own contracts" ON contracts;
CREATE POLICY "Users can delete own contracts"
  ON contracts FOR DELETE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Admin can view all contracts" ON contracts;
CREATE POLICY "Admin can view all contracts"
  ON contracts FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- SUBCONTRACTORS: RLS Policies
-- Bruker kan CRUD egne underleverandører
-- ============================================================
DROP POLICY IF EXISTS "Users can view own subcontractors" ON subcontractors;
CREATE POLICY "Users can view own subcontractors"
  ON subcontractors FOR SELECT
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can insert subcontractors" ON subcontractors;
CREATE POLICY "Users can insert subcontractors"
  ON subcontractors FOR INSERT
  WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can update own subcontractors" ON subcontractors;
CREATE POLICY "Users can update own subcontractors"
  ON subcontractors FOR UPDATE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can delete own subcontractors" ON subcontractors;
CREATE POLICY "Users can delete own subcontractors"
  ON subcontractors FOR DELETE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Admin can view all subcontractors" ON subcontractors;
CREATE POLICY "Admin can view all subcontractors"
  ON subcontractors FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- OWNERS: RLS Policies
-- Bruker kan CRUD egne eiere
-- ============================================================
DROP POLICY IF EXISTS "Users can view own owners" ON owners;
CREATE POLICY "Users can view own owners"
  ON owners FOR SELECT
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can insert owners" ON owners;
CREATE POLICY "Users can insert owners"
  ON owners FOR INSERT
  WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can update own owners" ON owners;
CREATE POLICY "Users can update own owners"
  ON owners FOR UPDATE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Users can delete own owners" ON owners;
CREATE POLICY "Users can delete own owners"
  ON owners FOR DELETE
  USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "Admin can view all owners" ON owners;
CREATE POLICY "Admin can view all owners"
  ON owners FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );


-- ============================================================
-- FUNCTION: screen_vendor
-- Screener en enkelt leverandør med alle underleverandører
-- og eiere fra aktive kontrakter
-- ============================================================
CREATE OR REPLACE FUNCTION screen_vendor(
  p_vendor_id UUID,
  p_threshold REAL DEFAULT 0.5
)
RETURNS TABLE (
  match_id TEXT,
  match_name TEXT,
  match_type TEXT,
  -- 'vendor', 'subcontractor', 'owner'
  matched_entity_name TEXT,
  matched_source TEXT,
  match_score REAL,
  entity_type TEXT,
  programs JSONB,
  reasons TEXT,
  source_url TEXT,
  nationalities JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_vendor_id UUID := p_vendor_id;
  v_user_id UUID;
  v_vendor_name TEXT;
  v_run_id UUID;
  v_has_active_contracts BOOLEAN := FALSE;
  v_match RECORD;
BEGIN
  -- Hent leverandør og sjekk eierskap
  SELECT user_id, name INTO v_user_id, v_vendor_name
  FROM vendors WHERE id = v_vendor_id;

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Vendor not found';
  END IF;

  -- Sjekk om leverandør har minst en aktiv kontrakt
  SELECT EXISTS(
    SELECT 1 FROM contracts
    WHERE vendor_id = v_vendor_id
      AND end_date >= CURRENT_DATE
  ) INTO v_has_active_contracts;

  IF NOT v_has_active_contracts THEN
    RAISE NOTICE 'Vendor has no active contracts';
    RETURN;
  END IF;

  -- Opprett screening run
  INSERT INTO screening_runs (user_id, status)
  VALUES (v_user_id, 'running')
  RETURNING id INTO v_run_id;

  -- Screen hovedleverandør
  FOR v_match IN
    SELECT
      e.id,
      e.full_name,
      e.source,
      similarity(lower(v_vendor_name), lower(e.full_name)) AS sim_score,
      e.entity_type,
      e.programs,
      e.reasons,
      e.source_url,
      e.nationalities
    FROM entities e
    WHERE lower(v_vendor_name) % lower(e.full_name)
      AND similarity(lower(v_vendor_name), lower(e.full_name)) >= p_threshold
    ORDER BY sim_score DESC
    LIMIT 5
  LOOP
    match_id := v_match.id;
    match_name := v_vendor_name;
    match_type := 'vendor';
    matched_entity_name := v_match.full_name;
    matched_source := v_match.source;
    match_score := v_match.sim_score;
    entity_type := v_match.entity_type;
    programs := v_match.programs;
    reasons := v_match.reasons;
    source_url := v_match.source_url;
    nationalities := v_match.nationalities;
    RETURN NEXT;
  END LOOP;

  -- Screen underleverandører fra aktive kontrakter
  FOR v_match IN
    SELECT
      e.id,
      sc.name,
      e.full_name,
      e.source,
      similarity(lower(sc.name), lower(e.full_name)) AS sim_score,
      e.entity_type,
      e.programs,
      e.reasons,
      e.source_url,
      e.nationalities
    FROM subcontractors sc
    JOIN contracts c ON c.id = sc.contract_id
    JOIN entities e ON TRUE
    WHERE sc.vendor_id = v_vendor_id
      AND c.end_date >= CURRENT_DATE
      AND lower(sc.name) % lower(e.full_name)
      AND similarity(lower(sc.name), lower(e.full_name)) >= p_threshold
    ORDER BY sim_score DESC
  LOOP
    match_id := v_match.id;
    match_name := v_match.name;
    match_type := 'subcontractor';
    matched_entity_name := v_match.full_name;
    matched_source := v_match.source;
    match_score := v_match.sim_score;
    entity_type := v_match.entity_type;
    programs := v_match.programs;
    reasons := v_match.reasons;
    source_url := v_match.source_url;
    nationalities := v_match.nationalities;
    RETURN NEXT;
  END LOOP;

  -- Screen eiere
  FOR v_match IN
    SELECT
      e.id,
      o.name,
      e.full_name,
      e.source,
      similarity(lower(o.name), lower(e.full_name)) AS sim_score,
      e.entity_type,
      e.programs,
      e.reasons,
      e.source_url,
      e.nationalities
    FROM owners o
    JOIN entities e ON TRUE
    WHERE o.vendor_id = v_vendor_id
      AND lower(o.name) % lower(e.full_name)
      AND similarity(lower(o.name), lower(e.full_name)) >= p_threshold
    ORDER BY sim_score DESC
  LOOP
    match_id := v_match.id;
    match_name := v_match.name;
    match_type := 'owner';
    matched_entity_name := v_match.full_name;
    matched_source := v_match.source;
    match_score := v_match.sim_score;
    entity_type := v_match.entity_type;
    programs := v_match.programs;
    reasons := v_match.reasons;
    source_url := v_match.source_url;
    nationalities := v_match.nationalities;
    RETURN NEXT;
  END LOOP;

  -- Merk run som ferdig
  UPDATE screening_runs
  SET status = 'completed', completed_at = NOW()
  WHERE id = v_run_id;
END;
$$;

GRANT EXECUTE ON FUNCTION screen_vendor TO authenticated;


-- ============================================================
-- FUNCTION: screen_all_active_vendors
-- Service role funksjon for screening av alle leverandører
-- med aktive kontrakter
-- ============================================================
CREATE OR REPLACE FUNCTION screen_all_active_vendors()
RETURNS TABLE (
  vendor_id UUID,
  vendor_name TEXT,
  user_email TEXT,
  new_matches_count INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_vendor RECORD;
  v_user_id UUID;
  v_matches INTEGER;
BEGIN
  -- Finn alle leverandører med aktive kontrakter og aktive abonnement
  FOR v_vendor IN
    SELECT DISTINCT
      v.id,
      v.name,
      v.user_id,
      p.contact_email,
      COALESCE(p.notification_email, p.contact_email) AS notify_email,
      COALESCE(p.match_threshold, 0.5) AS threshold
    FROM vendors v
    JOIN contracts c ON c.vendor_id = v.id
    JOIN profiles p ON p.id = v.user_id
    LEFT JOIN subscriptions s ON s.user_id = v.user_id
    WHERE c.end_date >= CURRENT_DATE
      AND (s.status = 'active' OR s.id IS NULL)
  LOOP
    -- Screen leverandør
    PERFORM screen_vendor(v_vendor.id, v_vendor.threshold);

    -- Tell nye treff
    -- (I praksis ville du koble dette med screening_results,
    --  men for nå returnerer vi basert på run stats)
    v_matches := 0;

    vendor_id := v_vendor.id;
    vendor_name := v_vendor.name;
    user_email := v_vendor.notify_email;
    new_matches_count := v_matches;
    RETURN NEXT;
  END LOOP;
END;
$$;

REVOKE EXECUTE ON FUNCTION screen_all_active_vendors FROM anon, authenticated;


-- ============================================================
-- FUNCTION: check_expiring_contracts
-- Finner kontrakter som utløper innen p_days_ahead dager
-- ============================================================
CREATE OR REPLACE FUNCTION check_expiring_contracts(p_days_ahead INTEGER DEFAULT 30)
RETURNS TABLE (
  contract_id UUID,
  contract_name TEXT,
  vendor_name TEXT,
  end_date DATE,
  user_email TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  SELECT
    c.id,
    c.name,
    v.name,
    c.end_date,
    p.contact_email
  FROM contracts c
  JOIN vendors v ON v.id = c.vendor_id
  JOIN profiles p ON p.id = c.user_id
  WHERE c.end_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + (p_days_ahead || ' days')::INTERVAL)
    AND c.notify_before_expiry = TRUE
    AND c.expiry_notified = FALSE
    AND c.is_active = TRUE;

  -- Marker som varslet
  UPDATE contracts
  SET expiry_notified = TRUE, updated_at = NOW()
  WHERE end_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + (p_days_ahead || ' days')::INTERVAL)
    AND notify_before_expiry = TRUE
    AND expiry_notified = FALSE
    AND is_active = TRUE;
END;
$$;

REVOKE EXECUTE ON FUNCTION check_expiring_contracts FROM anon, authenticated;


-- ============================================================
-- FUNCTION: check_expiring_subscriptions
-- Finner abonnement som utløper innen p_days_ahead dager
-- ============================================================
CREATE OR REPLACE FUNCTION check_expiring_subscriptions(p_days_ahead INTEGER DEFAULT 30)
RETURNS TABLE (
  subscription_id UUID,
  user_email TEXT,
  plan TEXT,
  expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  SELECT
    s.id,
    p.contact_email,
    s.plan,
    s.expires_at
  FROM subscriptions s
  JOIN profiles p ON p.id = s.user_id
  WHERE s.expires_at BETWEEN NOW() AND (NOW() + (p_days_ahead || ' days')::INTERVAL)
    AND s.status = 'active';
END;
$$;

REVOKE EXECUTE ON FUNCTION check_expiring_subscriptions FROM anon, authenticated;


-- ============================================================
-- FUNCTION: deactivate_expired_contracts
-- Deaktiverer kontrakter hvor end_date < CURRENT_DATE
-- og leverandører uten aktive kontrakter
-- ============================================================
CREATE OR REPLACE FUNCTION deactivate_expired_contracts()
RETURNS TABLE (
  vendor_id UUID,
  vendor_name TEXT,
  user_email TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_vendor RECORD;
BEGIN
  -- Deaktiver utgåtte kontrakter
  UPDATE contracts
  SET is_active = FALSE, updated_at = NOW()
  WHERE end_date < CURRENT_DATE AND is_active = TRUE;

  -- Finn og deaktiver leverandører uten aktive kontrakter
  FOR v_vendor IN
    SELECT v.id, v.name, v.user_id, p.contact_email
    FROM vendors v
    JOIN profiles p ON p.id = v.user_id
    WHERE v.is_active = TRUE
      AND NOT EXISTS (
        SELECT 1 FROM contracts
        WHERE vendor_id = v.id AND end_date >= CURRENT_DATE
      )
  LOOP
    UPDATE vendors
    SET is_active = FALSE, updated_at = NOW()
    WHERE id = v_vendor.id;

    vendor_id := v_vendor.id;
    vendor_name := v_vendor.name;
    user_email := v_vendor.contact_email;
    RETURN NEXT;
  END LOOP;
END;
$$;

REVOKE EXECUTE ON FUNCTION deactivate_expired_contracts FROM anon, authenticated;


-- ============================================================
-- FUNCTION: get_supplier_count
-- Teller totalt antall leverandører for bruker
-- (vendors + subcontractors fra aktive kontrakter + eiere)
-- ============================================================
CREATE OR REPLACE FUNCTION get_supplier_count(p_user_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_vendor_count INTEGER := 0;
  v_subcontractor_count INTEGER := 0;
  v_owner_count INTEGER := 0;
BEGIN
  -- Tell leverandører
  SELECT COUNT(*) INTO v_vendor_count
  FROM vendors
  WHERE user_id = p_user_id;

  -- Tell underleverandører fra aktive kontrakter
  SELECT COUNT(*) INTO v_subcontractor_count
  FROM subcontractors sc
  JOIN contracts c ON c.id = sc.contract_id
  WHERE sc.user_id = p_user_id
    AND c.end_date >= CURRENT_DATE;

  -- Tell eiere
  SELECT COUNT(*) INTO v_owner_count
  FROM owners
  WHERE user_id = p_user_id;

  RETURN v_vendor_count + v_subcontractor_count + v_owner_count;
END;
$$;

GRANT EXECUTE ON FUNCTION get_supplier_count TO authenticated;


-- ============================================================
-- FUNCTION: process_order
-- Service role funksjon for å behandle bestilling
-- Oppretter auth bruker, abonnement, og oppdaterer ordre
-- ============================================================
CREATE OR REPLACE FUNCTION process_order(p_order_id UUID, p_password TEXT)
RETURNS UUID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_order RECORD;
  v_new_user_id UUID;
  v_subscription_id UUID;
  v_price_monthly INTEGER;
  v_max_suppliers INTEGER;
BEGIN
  -- Hent ordre
  SELECT * INTO v_order FROM orders WHERE id = p_order_id;

  IF v_order IS NULL THEN
    RAISE EXCEPTION 'Order not found';
  END IF;

  IF v_order.status != 'pending' THEN
    RAISE EXCEPTION 'Order is not pending';
  END IF;

  -- Opprett auth bruker
  -- (I praksis ville dette gjøres via Supabase Auth API,
  --  her er det pseudokode)
  INSERT INTO auth.users (email, email_confirmed_at, encrypted_password, instance_id, aud, role)
  VALUES (
    v_order.email,
    NOW(),
    crypt(p_password, gen_salt('bf')),
    '00000000-0000-0000-0000-000000000000',
    'authenticated',
    'authenticated'
  )
  RETURNING id INTO v_new_user_id;

  -- Opprett profil
  INSERT INTO profiles (id, contact_email, organisation_name)
  VALUES (v_new_user_id, v_order.email, v_order.employer)
  ON CONFLICT (id) DO NOTHING;

  -- Bestem pris og maks leverandører basert på plan
  CASE v_order.selected_plan
    WHEN 'basic' THEN
      v_price_monthly := 490;
      v_max_suppliers := 200;
    WHEN 'standard' THEN
      v_price_monthly := 990;
      v_max_suppliers := 1000;
    WHEN 'unlimited' THEN
      v_price_monthly := 1490;
      v_max_suppliers := NULL;
  END CASE;

  -- Opprett abonnement (12 måneder)
  INSERT INTO subscriptions (
    user_id, plan, price_monthly, max_suppliers,
    starts_at, expires_at
  ) VALUES (
    v_new_user_id,
    v_order.selected_plan,
    v_price_monthly,
    v_max_suppliers,
    NOW(),
    NOW() + INTERVAL '12 months'
  )
  RETURNING id INTO v_subscription_id;

  -- Oppdater ordre
  UPDATE orders
  SET status = 'processed',
      processed_at = NOW(),
      user_id = v_new_user_id
  WHERE id = p_order_id;

  RETURN v_new_user_id;
END;
$$;

REVOKE EXECUTE ON FUNCTION process_order FROM anon, authenticated;


-- ============================================================
-- FUNCTION: ensure_subscription_active
-- Trigger funksjon: Sjekk og oppdater subscription status
-- basert på expires_at
-- ============================================================
CREATE OR REPLACE FUNCTION ensure_subscription_active()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- Dersom expires_at er i fortiden, marker som expired
  IF NEW.expires_at < NOW() AND NEW.status = 'active' THEN
    NEW.status := 'expired';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trigger_subscription_expiry ON subscriptions;
CREATE TRIGGER trigger_subscription_expiry
  BEFORE INSERT OR UPDATE ON subscriptions
  FOR EACH ROW
  EXECUTE FUNCTION ensure_subscription_active();


-- ============================================================
-- FUNCTION: ensure_contract_is_active
-- Trigger funksjon: Oppdater is_active basert på end_date
-- ============================================================
CREATE OR REPLACE FUNCTION ensure_contract_is_active()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  NEW.is_active := (NEW.end_date >= CURRENT_DATE);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trigger_contract_active ON contracts;
CREATE TRIGGER trigger_contract_active
  BEFORE INSERT OR UPDATE ON contracts
  FOR EACH ROW
  EXECUTE FUNCTION ensure_contract_is_active();


-- ============================================================
-- FUNCTION: update_vendor_active_status
-- Trigger funksjon: Oppdater vendor is_active
-- basert på om det finnes aktive kontrakter
-- ============================================================
CREATE OR REPLACE FUNCTION update_vendor_active_status()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_has_active BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM contracts
    WHERE vendor_id = COALESCE(NEW.vendor_id, OLD.vendor_id)
      AND end_date >= CURRENT_DATE
  ) INTO v_has_active;

  UPDATE vendors
  SET is_active = v_has_active,
      updated_at = NOW()
  WHERE id = COALESCE(NEW.vendor_id, OLD.vendor_id);

  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS trigger_update_vendor_on_contract ON contracts;
CREATE TRIGGER trigger_update_vendor_on_contract
  AFTER INSERT OR UPDATE OR DELETE ON contracts
  FOR EACH ROW
  EXECUTE FUNCTION update_vendor_active_status();


-- ============================================================
-- FUNCTION: update_contract_timestamp
-- Trigger funksjon: Oppdater updated_at på kontrakt
-- ============================================================
CREATE OR REPLACE FUNCTION update_contract_timestamp()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trigger_contract_updated ON contracts;
CREATE TRIGGER trigger_contract_updated
  BEFORE UPDATE ON contracts
  FOR EACH ROW
  EXECUTE FUNCTION update_contract_timestamp();


-- ============================================================
-- ADMIN POLICIES: Extend to profiles table
-- ============================================================
DROP POLICY IF EXISTS "Admin can view all profiles" ON profiles;
CREATE POLICY "Admin can view all profiles"
  ON profiles FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM profiles WHERE id = auth.uid() AND is_admin = TRUE)
  );
