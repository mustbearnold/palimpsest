-- 0029_wiki_schema_configs.sql
-- Tenant-owned versioned wiki schema configuration (spec 017 P4, AC10).
--
-- The schema configuration is tenant-owned and versioned (R11). A schema
-- amendment is a governed write: it carries a registered write policy
-- (001 R9) and records the amending principal. The registry follows the
-- surface-policy pattern (migration 0023): RLS FORCE, scope GUCs,
-- register + get routes. One row per (tenant_id, schema_version). No row
-- means the tenant has not amended the schema; the vault schema v1
-- description remains the implicit baseline (export.rs).

CREATE TABLE memory.wiki_schema_configs (
    tenant_id uuid NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    config jsonb NOT NULL CHECK (config <> 'null'::jsonb),
    amended_by_principal_id text NOT NULL CHECK (
        btrim(amended_by_principal_id) <> ''
    ),
    supersedes_version integer CHECK (
        supersedes_version IS NULL OR supersedes_version > 0
    ),
    write_policy_id text NOT NULL CHECK (btrim(write_policy_id) <> ''),
    write_policy_version text NOT NULL CHECK (btrim(write_policy_version) <> ''),
    idempotency_key_digest character(64) NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    PRIMARY KEY (tenant_id, schema_version),
    CONSTRAINT wiki_schema_configs_chain_order CHECK (
        supersedes_version IS NULL OR supersedes_version < schema_version
    )
);

CREATE INDEX wiki_schema_configs_scope_idx
    ON memory.wiki_schema_configs (tenant_id, schema_version);

ALTER TABLE memory.wiki_schema_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.wiki_schema_configs FORCE ROW LEVEL SECURITY;

CREATE POLICY wiki_schema_configs_tenant_scope_select
    ON memory.wiki_schema_configs
    FOR SELECT
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY wiki_schema_configs_tenant_scope_insert
    ON memory.wiki_schema_configs
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );
