CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

CREATE SCHEMA IF NOT EXISTS memory;

CREATE TABLE memory.episodes (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    episode_id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (btrim(kind) <> '' AND length(kind) <= 255),
    observed_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    writer_principal_id text NOT NULL CHECK (btrim(writer_principal_id) <> ''),
    source_type text NOT NULL CHECK (btrim(source_type) <> '' AND length(source_type) <= 255),
    source_uri text,
    external_id text,
    sensitivity text NOT NULL CHECK (btrim(sensitivity) <> '' AND length(sensitivity) <= 255),
    retention_policy_id text NOT NULL CHECK (
        btrim(retention_policy_id) <> '' AND length(retention_policy_id) <= 255
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) IS NOT NULL),
    payload_sha256 character(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    UNIQUE (tenant_id, subject_id, episode_id)
);

CREATE INDEX episodes_scope_recorded_idx
    ON memory.episodes (tenant_id, subject_id, recorded_at DESC, episode_id);

CREATE FUNCTION memory.reject_episode_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'episodes are append-only' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER episodes_reject_mutation
BEFORE UPDATE OR DELETE ON memory.episodes
FOR EACH ROW EXECUTE FUNCTION memory.reject_episode_mutation();

ALTER TABLE memory.episodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.episodes FORCE ROW LEVEL SECURITY;

CREATE POLICY episode_scope ON memory.episodes
    USING (
        tenant_id = current_setting('palimpsest.tenant_id', true)::uuid
        AND subject_id = current_setting('palimpsest.subject_id', true)::uuid
    )
    WITH CHECK (
        tenant_id = current_setting('palimpsest.tenant_id', true)::uuid
        AND subject_id = current_setting('palimpsest.subject_id', true)::uuid
    );
