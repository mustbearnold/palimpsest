-- Close the worker and fenced-purge RLS seams without granting ordinary
-- scoped queries broad table access. SECURITY DEFINER claim functions set a
-- transaction-local worker marker before their all-scope claim; normal owner
-- or runtime queries do not receive this policy path.

CREATE POLICY subject_lifecycles_worker_claim
ON memory.subject_lifecycles
FOR SELECT
USING (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.subject_lifecycles'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
);

CREATE POLICY deletion_operations_worker_claim
ON memory.deletion_operations
FOR ALL
USING (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.deletion_operations'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
)
WITH CHECK (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.deletion_operations'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
);

CREATE POLICY export_operations_worker_claim
ON memory.export_operations
FOR ALL
USING (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.export_operations'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
)
WITH CHECK (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.export_operations'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
);

CREATE POLICY export_manifest_items_worker_cleanup
ON memory.export_manifest_items
FOR DELETE
USING (
    current_user = pg_get_userbyid((
        SELECT relowner
        FROM pg_catalog.pg_class
        WHERE oid = 'memory.export_manifest_items'::pg_catalog.regclass
    ))
    AND current_setting('palimpsest.worker_claim', true)
        = 'palimpsest-worker-v1'
);

-- Subject content remains hidden from ordinary reads while a deletion is
-- fenced.  The deletion workflow marker is set only inside the leased,
-- operation-scoped purge or verification transaction.
ALTER POLICY episodes_active_subject ON memory.episodes
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY idempotency_receipts_active_subject ON memory.idempotency_receipts
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY facts_active_subject ON memory.facts
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY fact_revisions_active_subject ON memory.fact_revisions
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY fact_revision_evidence_active_subject ON memory.fact_revision_evidence
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY write_audit_receipts_active_subject ON memory.write_audit_receipts
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY outbox_intents_active_subject ON memory.outbox_intents
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY checkpoints_active_subject ON memory.checkpoints
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY checkpoint_revisions_active_subject ON memory.checkpoint_revisions
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY checkpoint_effect_intents_active_subject ON memory.checkpoint_effect_intents
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY checkpoint_effect_receipts_active_subject ON memory.checkpoint_effect_receipts
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY fact_revision_governance_active_subject ON memory.fact_revision_governance
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY fact_revision_search_documents_active_subject ON memory.fact_revision_search_documents
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY retrieval_receipts_active_subject ON memory.retrieval_receipts
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY retrieval_idempotency_reservations_active_subject
ON memory.retrieval_idempotency_reservations
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY retrieval_manifest_items_active_subject ON memory.retrieval_manifest_items
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

ALTER POLICY fact_revision_embedding_projections_active_subject
ON memory.fact_revision_embedding_projections
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

CREATE FUNCTION memory.renew_deletion_operation_lease(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_lease_seconds integer
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    renewed_until timestamptz;
BEGIN
    IF candidate_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'deletion operation lease must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion operation lease scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    UPDATE memory.deletion_operations AS operation
    SET worker_lease_expires_at = clock_timestamp()
        + make_interval(secs => candidate_lease_seconds),
        updated_at = clock_timestamp()
    WHERE operation.tenant_id = candidate_tenant_id
      AND operation.subject_id = candidate_subject_id
      AND operation.operation_id = candidate_operation_id
      AND operation.lifecycle_state IN ('draining', 'fenced', 'purging', 'retry_wait')
      AND operation.worker_lease_id = candidate_worker_id
      AND operation.worker_lease_expires_at > clock_timestamp()
    RETURNING operation.worker_lease_expires_at INTO renewed_until;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation worker lease is not held'
            USING ERRCODE = '55000';
    END IF;
    RETURN renewed_until;
END;
$$;

REVOKE ALL ON FUNCTION memory.renew_deletion_operation_lease(
    uuid, uuid, uuid, uuid, integer
)
FROM PUBLIC;

CREATE FUNCTION memory.renew_deletion_target_lease(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_target_key_digest character(64),
    candidate_target_lease_id uuid,
    candidate_lease_seconds integer
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    renewed_until timestamptz;
BEGIN
    IF candidate_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'deletion target lease must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion target lease scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    UPDATE memory.deletion_targets AS target
    SET lease_expires_at = clock_timestamp()
        + make_interval(secs => candidate_lease_seconds),
        updated_at = clock_timestamp()
    FROM memory.deletion_operations AS operation
    WHERE target.tenant_id = candidate_tenant_id
      AND target.subject_id = candidate_subject_id
      AND target.operation_id = candidate_operation_id
      AND target.target_key_digest = candidate_target_key_digest
      AND target.state = 'leased'
      AND target.lease_id = candidate_target_lease_id
      AND operation.tenant_id = target.tenant_id
      AND operation.subject_id = target.subject_id
      AND operation.operation_id = target.operation_id
      AND operation.lifecycle_state = 'purging'
      AND operation.worker_lease_id = candidate_worker_id
      AND operation.worker_lease_expires_at > clock_timestamp()
      AND target.lease_expires_at > clock_timestamp()
    RETURNING target.lease_expires_at INTO renewed_until;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion target lease is not held'
            USING ERRCODE = '55000';
    END IF;
    RETURN renewed_until;
END;
$$;

REVOKE ALL ON FUNCTION memory.renew_deletion_target_lease(
    uuid, uuid, uuid, uuid, character, uuid, integer
)
FROM PUBLIC;

-- Creation evidence is content-free and short-lived.  It exists during the
-- operation so a successful 202 has an explicit tombstone/audit seed; the
-- operation's ON DELETE CASCADE reduces these seed rows to the terminal
-- tombstone allowlist at completion.
CREATE TABLE memory.deletion_tombstone_seeds (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    scope_digest text NOT NULL CHECK (scope_digest ~ '^v[0-9]+:[0-9a-f]{64}$'),
    idempotency_key_digest character(64) NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_fingerprint_sha256 character(64) NOT NULL CHECK (
        request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    policy_version text NOT NULL CHECK (btrim(policy_version) <> ''),
    contract_schema_version integer NOT NULL CHECK (contract_schema_version >= 1),
    worker_release text NOT NULL CHECK (btrim(worker_release) <> ''),
    backup_policy_id text NOT NULL CHECK (btrim(backup_policy_id) <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id, operation_id),
    FOREIGN KEY (tenant_id, subject_id, operation_id)
        REFERENCES memory.deletion_operations (tenant_id, subject_id, operation_id)
        ON DELETE CASCADE
);

CREATE TABLE memory.deletion_audit_seeds (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    scope_digest text NOT NULL CHECK (scope_digest ~ '^v[0-9]+:[0-9a-f]{64}$'),
    event_type text NOT NULL CHECK (event_type = 'subject_deletion_requested'),
    policy_version text NOT NULL CHECK (btrim(policy_version) <> ''),
    contract_schema_version integer NOT NULL CHECK (contract_schema_version >= 1),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id, operation_id),
    FOREIGN KEY (tenant_id, subject_id, operation_id)
        REFERENCES memory.deletion_operations (tenant_id, subject_id, operation_id)
        ON DELETE CASCADE
);

ALTER TABLE memory.deletion_tombstone_seeds ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_tombstone_seeds FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_tombstone_seeds_scope
ON memory.deletion_tombstone_seeds
FOR ALL
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
)
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

ALTER TABLE memory.deletion_audit_seeds ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_audit_seeds FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_audit_seeds_scope
ON memory.deletion_audit_seeds
FOR ALL
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
)
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE FUNCTION memory.seed_deletion_evidence()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate_scope_digest text;
    candidate_idempotency_key_digest character(64);
BEGIN
    candidate_scope_digest := memory.deletion_scope_digest(
        NEW.tenant_id,
        NEW.subject_id
    );
    candidate_idempotency_key_digest := encode(
        sha256(convert_to(
            'palimpsest.deletion-idempotency/v1:'
            || NEW.principal_id || ':' || NEW.idempotency_key,
            'UTF8'
        )),
        'hex'
    );

    INSERT INTO memory.deletion_tombstone_seeds (
        tenant_id,
        subject_id,
        operation_id,
        scope_digest,
        idempotency_key_digest,
        request_fingerprint_sha256,
        policy_version,
        contract_schema_version,
        worker_release,
        backup_policy_id
    )
    SELECT NEW.tenant_id,
           NEW.subject_id,
           NEW.operation_id,
           candidate_scope_digest,
           candidate_idempotency_key_digest,
           NEW.request_fingerprint_sha256,
           'subject-delete/v1',
           1,
           'palimpsest-deletion-worker/v1',
           'isolated-until-expiry/operator-declared'
    ON CONFLICT DO NOTHING;

    INSERT INTO memory.deletion_audit_seeds (
        tenant_id,
        subject_id,
        operation_id,
        scope_digest,
        event_type,
        policy_version,
        contract_schema_version
    )
    VALUES (
        NEW.tenant_id,
        NEW.subject_id,
        NEW.operation_id,
        candidate_scope_digest,
        'subject_deletion_requested',
        'subject-delete/v1',
        1
    )
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE TRIGGER deletion_idempotency_seed_evidence
AFTER INSERT ON memory.deletion_idempotency_keys
FOR EACH ROW EXECUTE FUNCTION memory.seed_deletion_evidence();

REVOKE ALL ON FUNCTION memory.seed_deletion_evidence()
FROM PUBLIC;
