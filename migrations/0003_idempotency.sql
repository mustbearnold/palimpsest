CREATE TABLE memory.idempotency_receipts (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    operation_id text NOT NULL CHECK (btrim(operation_id) <> ''),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    state text NOT NULL CHECK (state IN ('in_progress', 'completed')),
    resource_episode_id uuid,
    resource_fact_id uuid,
    response_status smallint,
    response_body jsonb,
    response_etag text,
    response_location text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,

    PRIMARY KEY (tenant_id, principal_id, operation_id, idempotency_key),
    FOREIGN KEY (tenant_id, subject_id, resource_episode_id)
        REFERENCES memory.episodes (tenant_id, subject_id, episode_id),
    FOREIGN KEY (tenant_id, subject_id, resource_fact_id)
        REFERENCES memory.facts (tenant_id, subject_id, fact_id),
    CHECK (
        (
            state = 'in_progress'
            AND resource_episode_id IS NULL
            AND resource_fact_id IS NULL
            AND response_status IS NULL
            AND response_body IS NULL
            AND response_etag IS NULL
            AND response_location IS NULL
            AND completed_at IS NULL
        )
        OR
        (
            state = 'completed'
            AND (
                (resource_episode_id IS NOT NULL)::integer
                + (resource_fact_id IS NOT NULL)::integer
            ) = 1
            AND response_status BETWEEN 200 AND 299
            AND response_body IS NOT NULL
            AND response_etag IS NOT NULL
            AND response_location IS NOT NULL
            AND completed_at IS NOT NULL
        )
    )
);

CREATE FUNCTION memory.restrict_idempotency_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'idempotency receipts cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF OLD.state <> 'in_progress'
        OR NEW.state <> 'completed'
        OR OLD.tenant_id <> NEW.tenant_id
        OR OLD.subject_id <> NEW.subject_id
        OR OLD.principal_id <> NEW.principal_id
        OR OLD.operation_id <> NEW.operation_id
        OR OLD.idempotency_key <> NEW.idempotency_key
        OR OLD.request_fingerprint <> NEW.request_fingerprint
        OR (NEW.resource_episode_id IS NULL AND NEW.resource_fact_id IS NULL)
        OR NEW.response_status IS NULL
        OR NEW.response_body IS NULL
        OR NEW.response_etag IS NULL
        OR NEW.response_location IS NULL
        OR NEW.completed_at IS NULL
    THEN
        RAISE EXCEPTION 'invalid idempotency receipt transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER idempotency_receipts_restrict_mutation
BEFORE UPDATE OR DELETE ON memory.idempotency_receipts
FOR EACH ROW EXECUTE FUNCTION memory.restrict_idempotency_receipt_mutation();

ALTER TABLE memory.idempotency_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.idempotency_receipts FORCE ROW LEVEL SECURITY;

CREATE POLICY idempotency_receipt_scope ON memory.idempotency_receipts
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );
