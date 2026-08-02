-- Durable canonical-history export reservations and immutable membership.
-- Payloads stay in the source tables and the package store; these tables hold
-- only operation state and the identifiers/digests needed to re-materialize it.

CREATE TABLE memory.export_operations (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    export_id uuid NOT NULL CHECK (uuid_extract_version(export_id) = 7),
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    allowed_sensitivities text[] NOT NULL DEFAULT '{}',
    profile text NOT NULL CHECK (btrim(profile) <> '' AND length(profile) <= 255),
    idempotency_key text NOT NULL CHECK (
        btrim(idempotency_key) <> '' AND length(idempotency_key) <= 255
    ),
    request_fingerprint_sha256 character(64) NOT NULL CHECK (
        request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    authorization_scope_sha256 character(64) NOT NULL CHECK (
        authorization_scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    state text NOT NULL CHECK (
        state IN ('queued', 'materializing', 'ready', 'failed', 'revoked', 'expired')
    ),
    status_version bigint NOT NULL DEFAULT 1 CHECK (status_version > 0),
    worker_lease_id uuid,
    worker_lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(updated_at)),
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    content_sha256 character(64) CHECK (
        content_sha256 IS NULL OR content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    package_size_bytes bigint CHECK (package_size_bytes IS NULL OR package_size_bytes >= 0),
    record_count bigint CHECK (record_count IS NULL OR record_count >= 0),
    package_cleanup_completed_at timestamptz,
    failure_code text CHECK (
        failure_code IS NULL
        OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    PRIMARY KEY (tenant_id, subject_id, export_id),
    UNIQUE (tenant_id, subject_id, principal_id, idempotency_key)
);

COMMENT ON TABLE memory.export_operations IS
    'Content-free export operation state. Payloads are materialized separately.';

CREATE INDEX export_operations_claim_idx
    ON memory.export_operations (state, worker_lease_expires_at, created_at, export_id);

CREATE TABLE memory.export_manifest_items (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    export_id uuid NOT NULL,
    record_kind text NOT NULL CHECK (
        record_kind IN ('episode', 'checkpoint', 'fact_revision', 'procedure', 'artifact_reference')
    ),
    record_id uuid NOT NULL,
    recorded_at timestamptz NOT NULL CHECK (isfinite(recorded_at)),
    source_content_sha256 character(64) NOT NULL CHECK (
        source_content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    PRIMARY KEY (tenant_id, subject_id, export_id, record_kind, record_id),
    FOREIGN KEY (tenant_id, subject_id, export_id)
        REFERENCES memory.export_operations (tenant_id, subject_id, export_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE memory.export_manifest_items IS
    'Immutable authorized export membership; it contains no record payload.';

CREATE INDEX export_manifest_items_order_idx
    ON memory.export_manifest_items (
        tenant_id, subject_id, export_id, record_kind, recorded_at, record_id
    );

CREATE FUNCTION memory.reject_export_manifest_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    IF NOT memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id)
       AND current_setting('palimpsest.export_expiration_workflow', true)
           IS DISTINCT FROM 'true'
    THEN
        RAISE EXCEPTION 'export manifests are immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER export_manifest_items_reject_mutation
BEFORE UPDATE OR DELETE ON memory.export_manifest_items
FOR EACH ROW EXECUTE FUNCTION memory.reject_export_manifest_mutation();

ALTER TABLE memory.export_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.export_operations FORCE ROW LEVEL SECURITY;
CREATE POLICY export_operations_scope ON memory.export_operations
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.export_manifest_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.export_manifest_items FORCE ROW LEVEL SECURITY;
CREATE POLICY export_manifest_items_scope ON memory.export_manifest_items
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE FUNCTION memory.claim_next_export_operation(
    candidate_worker_lease_id uuid,
    lease_seconds integer
)
RETURNS TABLE(
    tenant_id uuid,
    subject_id uuid,
    export_id uuid,
    principal_id text,
    worker_lease_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.export_operations%ROWTYPE;
BEGIN
    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    IF lease_seconds <= 0 THEN
        RAISE EXCEPTION 'export worker lease must be positive'
            USING ERRCODE = '22023';
    END IF;

    SELECT operation.*
    INTO candidate
    FROM memory.export_operations AS operation
    WHERE operation.expires_at > clock_timestamp()
      AND (
          operation.state = 'queued'
          OR (
              operation.state = 'materializing'
              AND (
                  operation.worker_lease_expires_at IS NULL
                  OR operation.worker_lease_expires_at <= clock_timestamp()
              )
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM memory.subject_lifecycles AS lifecycle
          WHERE lifecycle.tenant_id = operation.tenant_id
            AND lifecycle.subject_id = operation.subject_id
            AND lifecycle.lifecycle_state <> 'active'
      )
    ORDER BY operation.created_at, operation.export_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.export_operations
    SET state = 'materializing',
        status_version = status_version + 1,
        worker_lease_id = candidate_worker_lease_id,
        worker_lease_expires_at = clock_timestamp()
            + make_interval(secs => lease_seconds),
        updated_at = clock_timestamp()
    WHERE memory.export_operations.tenant_id = candidate.tenant_id
      AND memory.export_operations.subject_id = candidate.subject_id
      AND memory.export_operations.export_id = candidate.export_id;

    RETURN QUERY
    SELECT candidate.tenant_id,
           candidate.subject_id,
           candidate.export_id,
           candidate.principal_id,
           candidate_worker_lease_id;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_export_operation(uuid, integer)
FROM PUBLIC;

CREATE FUNCTION memory.claim_next_expired_export_operation(
    candidate_worker_lease_id uuid,
    lease_seconds integer
)
RETURNS TABLE(
    tenant_id uuid,
    subject_id uuid,
    export_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.export_operations%ROWTYPE;
BEGIN
    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    IF lease_seconds <= 0 THEN
        RAISE EXCEPTION 'export cleanup worker lease must be positive'
            USING ERRCODE = '22023';
    END IF;

    SELECT operation.*
    INTO candidate
    FROM memory.export_operations AS operation
    WHERE operation.package_cleanup_completed_at IS NULL
      AND (
          operation.state IN ('failed', 'revoked', 'expired')
          OR (
              operation.state NOT IN ('failed', 'revoked', 'expired')
              AND operation.expires_at <= clock_timestamp()
          )
      )
      AND (
          operation.worker_lease_id IS NULL
          OR operation.worker_lease_expires_at IS NULL
          OR operation.worker_lease_expires_at <= clock_timestamp()
      )
    ORDER BY operation.expires_at, operation.export_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    PERFORM set_config('palimpsest.export_expiration_workflow', 'true', true);

    DELETE FROM memory.export_manifest_items
    WHERE memory.export_manifest_items.tenant_id = candidate.tenant_id
      AND memory.export_manifest_items.subject_id = candidate.subject_id
      AND memory.export_manifest_items.export_id = candidate.export_id;

    UPDATE memory.export_operations
    SET state = CASE
            WHEN candidate.state IN ('failed', 'revoked') THEN candidate.state
            ELSE 'expired'
        END,
        status_version = CASE
            WHEN candidate.state IN ('failed', 'revoked', 'expired') THEN status_version
            ELSE status_version + 1
        END,
        worker_lease_id = candidate_worker_lease_id,
        worker_lease_expires_at = clock_timestamp()
            + make_interval(secs => lease_seconds),
        content_sha256 = NULL,
        package_size_bytes = NULL,
        record_count = NULL,
        updated_at = clock_timestamp()
    WHERE memory.export_operations.tenant_id = candidate.tenant_id
      AND memory.export_operations.subject_id = candidate.subject_id
      AND memory.export_operations.export_id = candidate.export_id;

    RETURN QUERY
    SELECT candidate.tenant_id, candidate.subject_id, candidate.export_id;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_expired_export_operation(uuid, integer)
FROM PUBLIC;
