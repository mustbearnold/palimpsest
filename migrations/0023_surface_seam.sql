-- 0023_surface_seam.sql
--
-- Proactive surfacing (spec 012, #45): the opt-in surface policy registry
-- and the idempotent surface-response store.
--
-- The registry mirrors the consolidation policy pattern (migration 0022):
-- RLS FORCE, scope GUCs, register + get routes. One row per
-- (tenant_id, host_id, principal_id). No row means an empty bundle
-- (fail closed, D2).
--
-- The response store mirrors the recall contract (spec 002): the service
-- stores each evaluated bundle for idempotent replay. Keyed idempotency:
-- a unique constraint on (tenant_id, host_id, principal_id,
-- idempotency_key) with a request-fingerprint compare. A reused key with a
-- different body returns 409 IdempotencyKeyReused (A8).

CREATE TABLE memory.surface_policies (
    tenant_id uuid NOT NULL,
    host_id text NOT NULL CHECK (
        btrim(host_id) <> '' AND length(host_id) <= 255
    ),
    principal_id text NOT NULL CHECK (
        btrim(principal_id) <> '' AND length(principal_id) <= 255
    ),
    enabled boolean NOT NULL DEFAULT true,
    max_items smallint NOT NULL DEFAULT 20 CHECK (max_items BETWEEN 1 AND 50),
    max_context_tokens integer NOT NULL DEFAULT 4096
        CHECK (max_context_tokens BETWEEN 1 AND 1048576),
    max_result_tokens integer NOT NULL DEFAULT 2048
        CHECK (max_result_tokens BETWEEN 1 AND 1048576),
    sensitivity_ceiling text CHECK (
        sensitivity_ceiling IS NULL
        OR (
            btrim(sensitivity_ceiling) <> ''
            AND length(sensitivity_ceiling) <= 255
        )
    ),
    window_from timestamptz CHECK (
        window_from IS NULL OR isfinite(window_from)
    ),
    window_until timestamptz CHECK (
        window_until IS NULL OR isfinite(window_until)
    ),
    created_by_principal_id text NOT NULL CHECK (
        btrim(created_by_principal_id) <> ''
    ),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(updated_at)),
    PRIMARY KEY (tenant_id, host_id, principal_id),
    CONSTRAINT surface_policies_window_order CHECK (
        window_from IS NULL
        OR window_until IS NULL
        OR window_from < window_until
    )
);

CREATE TABLE memory.surface_responses (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    surface_id uuid NOT NULL,
    host_id text NOT NULL CHECK (
        btrim(host_id) <> '' AND length(host_id) <= 255
    ),
    principal_id text NOT NULL CHECK (
        btrim(principal_id) <> '' AND length(principal_id) <= 255
    ),
    idempotency_key_digest character(64) NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    policy_sha256 character(64) CHECK (
        policy_sha256 IS NULL OR policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    bundle_sha256 character(64) NOT NULL CHECK (
        bundle_sha256 ~ '^[0-9a-f]{64}$'
    ),
    item_count smallint NOT NULL CHECK (item_count >= 0),
    truncated boolean NOT NULL DEFAULT false,
    context_terms text[] NOT NULL DEFAULT '{}'::text[],
    evaluated_at timestamptz NOT NULL CHECK (isfinite(evaluated_at)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(recorded_at)),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, surface_id),
    UNIQUE (tenant_id, host_id, principal_id, idempotency_key_digest)
);

CREATE TABLE memory.surface_response_items (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    surface_id uuid NOT NULL,
    ordinal smallint NOT NULL CHECK (ordinal >= 0),
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    namespace text NOT NULL CHECK (btrim(namespace) <> ''),
    fact_key text NOT NULL CHECK (btrim(fact_key) <> ''),
    value jsonb NOT NULL CHECK (value <> 'null'::jsonb),
    confidence double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    sensitivity text NOT NULL CHECK (btrim(sensitivity) <> ''),
    lexical_score double precision NOT NULL CHECK (lexical_score >= 0),
    content_sha256 character(64) NOT NULL CHECK (
        content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    item_sha256 character(64) NOT NULL CHECK (
        item_sha256 ~ '^[0-9a-f]{64}$'
    ),
    PRIMARY KEY (tenant_id, subject_id, surface_id, ordinal),
    CONSTRAINT surface_response_items_response_fkey
    FOREIGN KEY (tenant_id, subject_id, surface_id)
        REFERENCES memory.surface_responses (
            tenant_id, subject_id, surface_id
        ) ON DELETE CASCADE
);

CREATE INDEX surface_responses_scope_idx
    ON memory.surface_responses (tenant_id, subject_id, surface_id);

ALTER TABLE memory.surface_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.surface_policies FORCE ROW LEVEL SECURITY;
ALTER TABLE memory.surface_responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.surface_responses FORCE ROW LEVEL SECURITY;
ALTER TABLE memory.surface_response_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.surface_response_items FORCE ROW LEVEL SECURITY;

CREATE POLICY surface_policy_tenant_scope_select
    ON memory.surface_policies
    FOR SELECT
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY surface_policy_tenant_scope_insert
    ON memory.surface_policies
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY surface_policy_tenant_scope_update
    ON memory.surface_policies
    FOR UPDATE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    )
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY surface_policy_tenant_scope_delete
    ON memory.surface_policies
    FOR DELETE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY surface_responses_scope_select
    ON memory.surface_responses
    FOR SELECT
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY surface_responses_scope_insert
    ON memory.surface_responses
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY surface_responses_active_subject
    ON memory.surface_responses AS RESTRICTIVE
    FOR ALL
    USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
    WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY surface_response_items_scope_select
    ON memory.surface_response_items
    FOR SELECT
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY surface_response_items_scope_insert
    ON memory.surface_response_items
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY surface_response_items_active_subject
    ON memory.surface_response_items AS RESTRICTIVE
    FOR ALL
    USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
    WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));
