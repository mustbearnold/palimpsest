-- Durable scoped-deletion operations (issue #31).
--
-- A deletion operation is a subject-scoped, idempotent workflow that:
--   1. commits the monotonic subject fence, operation, request fingerprint,
--      target ledger, audit seed, and outbox intent in one serializable
--      transaction;
--   2. drains and proves subject content leases (draining -> fenced);
--   3. purges derived projections/caches/exports/artifacts before canonical
--      and provenance rows (purging);
--   4. runs independent negative checks (verifying);
--   5. records a content-free tombstone and closes the fence (completed), or
--      fails closed with permanent fencing after bounded retries (failed).
--
-- Append-only triggers on canonical tables are relaxed ONLY for rows being
-- removed by a live deletion worker (see memory.deletion_workflow_allows).

-- HMAC-backed scope digests are retained only in the compact tombstone. The
-- key is generated once during migration and is not readable by the runtime
-- role; rotating it requires an explicit policy migration.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE memory.deletion_scope_keys (
    key_version text PRIMARY KEY CHECK (key_version ~ '^v[0-9]+$'),
    key_material bytea NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO memory.deletion_scope_keys (key_version, key_material)
VALUES ('v1', public.gen_random_bytes(32));

REVOKE ALL ON TABLE memory.deletion_scope_keys FROM PUBLIC;

CREATE FUNCTION memory.deletion_scope_digest(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
    SELECT key.key_version || ':' || encode(
        public.hmac(
            convert_to(
                'palimpsest.subject-scope/v1:'
                || candidate_tenant_id::text || ':' || candidate_subject_id::text,
                'UTF8'
            ),
            key.key_material,
            'sha256'
        ),
        'hex'
    )
    FROM memory.deletion_scope_keys AS key
    WHERE key.active
    ORDER BY key.key_version DESC
    LIMIT 1
$$;

REVOKE ALL ON FUNCTION memory.deletion_scope_digest(uuid, uuid) FROM PUBLIC;

-- Relax append-only guards for rows removed by the deletion workflow.
CREATE OR REPLACE FUNCTION memory.reject_episode_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       OR NOT memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id)
    THEN
        RAISE EXCEPTION 'episodes are append-only' USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION memory.reject_fact_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- These registries are global policy artifacts rather than subject-scoped
    -- history.  Keep the shared trigger safe for them after this migration
    -- replaces the pre-deletion append-only function.
    IF TG_TABLE_NAME IN (
        'fact_retention_policies',
        'search_projection_schemas',
        'lexical_retrieval_policies',
        'recency_profiles',
        'fact_retrieval_metadata_policies'
    ) THEN
        RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE'
        OR NOT memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id)
    THEN
        RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION memory.reject_checkpoint_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       OR NOT memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id)
    THEN
        RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION memory.restrict_idempotency_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id) THEN
            RETURN OLD;
        END IF;
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
        OR (
            (NEW.resource_episode_id IS NOT NULL)::integer
            + (NEW.resource_fact_id IS NOT NULL)::integer
            + (NEW.resource_checkpoint_id IS NOT NULL)::integer
        ) <> 1
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

CREATE OR REPLACE FUNCTION memory.restrict_fact_revision_governance_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id) THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'fact revision governance cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF OLD.tenant_id <> NEW.tenant_id
        OR OLD.subject_id <> NEW.subject_id
        OR OLD.case_id <> NEW.case_id
        OR OLD.fact_id <> NEW.fact_id
        OR OLD.revision_id <> NEW.revision_id
        OR OLD.retention_policy_id <> NEW.retention_policy_id
        OR OLD.retention_expires_at IS DISTINCT FROM NEW.retention_expires_at
        OR OLD.recency_profile_id <> NEW.recency_profile_id
        OR OLD.recency_profile_version <> NEW.recency_profile_version
        OR OLD.recency_profile_sha256 <> NEW.recency_profile_sha256
        OR OLD.recency_anchor_at <> NEW.recency_anchor_at
        OR OLD.importance <> NEW.importance
        OR OLD.metadata_policy_id <> NEW.metadata_policy_id
        OR OLD.metadata_policy_version <> NEW.metadata_policy_version
        OR OLD.metadata_policy_sha256 <> NEW.metadata_policy_sha256
        OR OLD.schema_version <> NEW.schema_version
        OR NOT (
            (OLD.lifecycle_state = 'active' AND NEW.lifecycle_state = 'deletion_pending')
            OR (
                OLD.lifecycle_state = 'deletion_pending'
                AND NEW.lifecycle_state = 'deleted'
            )
        )
    THEN
        RAISE EXCEPTION 'invalid fact revision governance transition'
            USING ERRCODE = '55000';
    END IF;

    NEW.lifecycle_changed_at := greatest(
        clock_timestamp(),
        OLD.lifecycle_changed_at + interval '1 microsecond'
    );
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION memory.prepare_checkpoint_head_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    revision_case_id uuid;
    revision_number bigint;
    revision_parent_id uuid;
    revision_recorded_at timestamptz;
    revision_expires_at timestamptz;
    revision_retention_policy_id text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id) THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'checkpoint heads cannot be deleted' USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF OLD.tenant_id <> NEW.tenant_id
            OR OLD.subject_id <> NEW.subject_id
            OR OLD.case_id <> NEW.case_id
            OR OLD.agent_id <> NEW.agent_id
            OR OLD.thread_id <> NEW.thread_id
            OR OLD.checkpoint_id <> NEW.checkpoint_id
            OR OLD.created_at <> NEW.created_at
            OR OLD.schema_version <> NEW.schema_version
            OR OLD.head_revision_id = NEW.head_revision_id
        THEN
            RAISE EXCEPTION 'invalid checkpoint head transition' USING ERRCODE = '55000';
        END IF;
    END IF;

    SELECT
        revision.case_id,
        revision.revision_number,
        revision.parent_revision_id,
        revision.recorded_at,
        revision.expires_at,
        revision.retention_policy_id
    INTO
        revision_case_id,
        revision_number,
        revision_parent_id,
        revision_recorded_at,
        revision_expires_at,
        revision_retention_policy_id
    FROM memory.checkpoint_revisions AS revision
    WHERE revision.tenant_id = NEW.tenant_id
      AND revision.subject_id = NEW.subject_id
      AND revision.agent_id = NEW.agent_id
      AND revision.thread_id = NEW.thread_id
      AND revision.checkpoint_id = NEW.checkpoint_id
      AND revision.revision_id = NEW.head_revision_id;

    IF revision_number IS NULL THEN
        RAISE EXCEPTION 'the checkpoint head revision does not exist in this lineage'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'checkpoints_head_revision_fkey';
    END IF;

    IF revision_case_id <> NEW.case_id THEN
        RAISE EXCEPTION 'the checkpoint head revision belongs to a different case'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'checkpoint_head_case_matches_revision';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF revision_number <> 1 OR revision_parent_id IS NOT NULL THEN
            RAISE EXCEPTION 'the first checkpoint head must be the root revision'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'checkpoint_first_head_is_root';
        END IF;

        NEW.created_at := revision_recorded_at;
    ELSE
        IF revision_parent_id IS DISTINCT FROM OLD.head_revision_id
            OR revision_number <> OLD.head_revision_number + 1
        THEN
            RAISE EXCEPTION 'a checkpoint head must advance to its direct successor'
                USING ERRCODE = '40001',
                      CONSTRAINT = 'checkpoint_head_advances_linearly';
        END IF;
    END IF;

    NEW.head_revision_number := revision_number;
    NEW.retention_policy_id := revision_retention_policy_id;
    NEW.expires_at := revision_expires_at;
    NEW.updated_at := revision_recorded_at;

    RETURN NEW;
END;
$$;

CREATE TABLE memory.deletion_operations (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN (
            'draining', 'fenced', 'purging', 'retry_wait',
            'verifying', 'completed', 'failed', 'expired'
        )
    ),
    state_version bigint NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    worker_lease_id uuid,
    worker_lease_expires_at timestamptz,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    retry_at timestamptz,
    failure_reason text CHECK (
        failure_reason IS NULL
        OR (
            length(failure_reason) BETWEEN 1 AND 200
            AND failure_reason ~ '^[a-z0-9 ._-]+$'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    repair_count integer NOT NULL DEFAULT 0 CHECK (repair_count >= 0),
    last_repair_reason text CHECK (
        last_repair_reason IS NULL
        OR last_repair_reason ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    last_repaired_at timestamptz,
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    PRIMARY KEY (tenant_id, subject_id, operation_id)
);

CREATE UNIQUE INDEX deletion_operations_one_active_subject
    ON memory.deletion_operations (tenant_id, subject_id)
    WHERE lifecycle_state NOT IN ('completed', 'failed', 'expired');

CREATE FUNCTION memory.deletion_target_key_digest(candidate_target_name text)
RETURNS character(64)
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog, memory, pg_temp
AS $$
    SELECT encode(
        sha256(convert_to('palimpsest.deletion-target/v1:' || candidate_target_name, 'UTF8')),
        'hex'
    )
$$;

REVOKE ALL ON FUNCTION memory.deletion_target_key_digest(text) FROM PUBLIC;

CREATE TABLE memory.deletion_targets (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    target_name text NOT NULL CHECK (
        target_name IN ('canonical', 'projections', 'caches', 'exports', 'artifacts')
    ),
    target_key_digest character(64) NOT NULL CHECK (
        target_key_digest ~ '^[0-9a-f]{64}$'
    ),
    capability text NOT NULL CHECK (
        capability IN ('configured', 'not_configured')
    ),
    state text NOT NULL CHECK (
        state IN ('pending', 'leased', 'done', 'failed', 'not_configured')
    ),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_id uuid,
    lease_expires_at timestamptz,
    effect_receipt_sha256 character(64) CHECK (
        effect_receipt_sha256 IS NULL OR effect_receipt_sha256 ~ '^[0-9a-f]{64}$'
    ),
    sanitized_error text CHECK (
        sanitized_error IS NULL
        OR (
            length(sanitized_error) BETWEEN 1 AND 200
            AND sanitized_error ~ '^[a-z0-9 ._-]+$'
        )
    ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id, operation_id, target_name, target_key_digest),
    FOREIGN KEY (tenant_id, subject_id, operation_id)
        REFERENCES memory.deletion_operations (tenant_id, subject_id, operation_id)
        ON DELETE RESTRICT
);

CREATE TABLE memory.deletion_tombstones (
    tenant_id uuid NOT NULL,
    scope_digest text NOT NULL CHECK (
        scope_digest ~ '^v[0-9]+:[0-9a-f]{64}$'
    ),
    operation_id uuid NOT NULL,
    lifecycle_state text NOT NULL CHECK (lifecycle_state = 'completed'),
    state_version bigint NOT NULL CHECK (state_version >= 1),
    idempotency_key_digest character(64) NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_fingerprint_sha256 character(64) NOT NULL CHECK (
        request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    policy_version text NOT NULL CHECK (btrim(policy_version) <> ''),
    contract_schema_version integer NOT NULL CHECK (contract_schema_version >= 1),
    worker_release text NOT NULL CHECK (btrim(worker_release) <> ''),
    target_summary jsonb NOT NULL CHECK (jsonb_typeof(target_summary) = 'array'),
    verification_digest character(64) NOT NULL CHECK (
        verification_digest ~ '^[0-9a-f]{64}$'
    ),
    backup_policy_id text NOT NULL CHECK (btrim(backup_policy_id) <> ''),
    earliest_backup_expiry timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    PRIMARY KEY (tenant_id, operation_id),
    UNIQUE (tenant_id, scope_digest, operation_id)
);

CREATE TABLE memory.deletion_idempotency_keys (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    operation_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_fingerprint_sha256 character(64) NOT NULL CHECK (
        request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, principal_id, idempotency_key),
    FOREIGN KEY (tenant_id, subject_id, operation_id)
        REFERENCES memory.deletion_operations (tenant_id, subject_id, operation_id)
        ON DELETE RESTRICT
);

ALTER TABLE memory.deletion_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_operations FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_operations_select_scope
ON memory.deletion_operations
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_operations_insert_scope
ON memory.deletion_operations
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_operations_update_scope
ON memory.deletion_operations
FOR UPDATE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
)
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_operations_delete_scope
ON memory.deletion_operations
FOR DELETE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

ALTER TABLE memory.deletion_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_targets FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_targets_select_scope
ON memory.deletion_targets
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_targets_insert_scope
ON memory.deletion_targets
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_targets_update_scope
ON memory.deletion_targets
FOR UPDATE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
)
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);
CREATE POLICY deletion_targets_delete_scope
ON memory.deletion_targets
FOR DELETE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

ALTER TABLE memory.deletion_idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_idempotency_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_idempotency_keys_select_scope
ON memory.deletion_idempotency_keys
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
);
CREATE POLICY deletion_idempotency_keys_insert_scope
ON memory.deletion_idempotency_keys
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
);

ALTER TABLE memory.deletion_tombstones ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.deletion_tombstones FORCE ROW LEVEL SECURITY;
CREATE POLICY deletion_tombstones_select_scope
ON memory.deletion_tombstones
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND scope_digest = NULLIF(current_setting('palimpsest.scope_digest', true), '')
);
CREATE POLICY deletion_tombstones_insert_scope
ON memory.deletion_tombstones
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND scope_digest = NULLIF(current_setting('palimpsest.scope_digest', true), '')
);

-- True only while a live deletion worker is purging the given subject scope.
-- This is the single gate that relaxes the append-only triggers below.
CREATE FUNCTION memory.deletion_workflow_allows(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
    SELECT
        NULLIF(current_setting('palimpsest.deletion_workflow', true), '') IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM memory.deletion_operations AS operation
            JOIN memory.subject_lifecycles AS lifecycle
                USING (tenant_id, subject_id)
            WHERE operation.tenant_id = candidate_tenant_id
              AND operation.subject_id = candidate_subject_id
              AND operation.operation_id =
                  NULLIF(current_setting('palimpsest.deletion_workflow', true), '')::uuid
              AND operation.lifecycle_state IN ('purging', 'verifying')
              AND operation.worker_lease_id IS NOT NULL
              AND lifecycle.lifecycle_state = 'deletion_pending'
        )
$$;

REVOKE ALL ON FUNCTION memory.deletion_workflow_allows(uuid, uuid)
FROM PUBLIC;

-- Outbox intents gain a deletion resource variant so the fenced operation is
-- durably observable as job intent before any success response is emitted.
ALTER TABLE memory.outbox_intents
    ADD COLUMN resource_deletion_operation_id uuid,
    ADD CONSTRAINT outbox_intents_deletion_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        resource_deletion_operation_id
    ) REFERENCES memory.deletion_operations (
        tenant_id,
        subject_id,
        operation_id
    ),
    DROP CONSTRAINT outbox_intents_resource_check,
    ADD CONSTRAINT outbox_intents_resource_check CHECK (
        (
            resource_episode_id IS NOT NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
            AND resource_deletion_operation_id IS NULL
        )
        OR
        (
            resource_episode_id IS NULL
            AND resource_fact_id IS NOT NULL
            AND resource_revision_id IS NOT NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
            AND resource_deletion_operation_id IS NULL
        )
        OR
        (
            resource_episode_id IS NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
            AND resource_checkpoint_id IS NOT NULL
            AND resource_checkpoint_revision_id IS NOT NULL
            AND resource_checkpoint_agent_id IS NOT NULL
            AND resource_checkpoint_thread_id IS NOT NULL
            AND resource_deletion_operation_id IS NULL
        )
        OR
        (
            resource_episode_id IS NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
            AND resource_deletion_operation_id IS NOT NULL
        )
    );

CREATE OR REPLACE FUNCTION memory.restrict_outbox_intent_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF memory.deletion_workflow_allows(OLD.tenant_id, OLD.subject_id) THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'outbox intents cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF OLD.published_at IS NOT NULL
        OR NEW.published_at IS NULL
        OR OLD.tenant_id <> NEW.tenant_id
        OR OLD.subject_id <> NEW.subject_id
        OR OLD.case_id <> NEW.case_id
        OR OLD.intent_id <> NEW.intent_id
        OR OLD.event_type <> NEW.event_type
        OR OLD.resource_episode_id IS DISTINCT FROM NEW.resource_episode_id
        OR OLD.resource_fact_id IS DISTINCT FROM NEW.resource_fact_id
        OR OLD.resource_revision_id IS DISTINCT FROM NEW.resource_revision_id
        OR OLD.resource_checkpoint_agent_id IS DISTINCT FROM NEW.resource_checkpoint_agent_id
        OR OLD.resource_checkpoint_thread_id IS DISTINCT FROM NEW.resource_checkpoint_thread_id
        OR OLD.resource_checkpoint_id IS DISTINCT FROM NEW.resource_checkpoint_id
        OR OLD.resource_checkpoint_revision_id IS DISTINCT FROM NEW.resource_checkpoint_revision_id
        OR OLD.resource_deletion_operation_id IS DISTINCT FROM NEW.resource_deletion_operation_id
        OR OLD.payload <> NEW.payload
        OR OLD.created_at <> NEW.created_at
    THEN
        RAISE EXCEPTION 'invalid outbox intent transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.classify_deletion_error(sqlstate text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN left(coalesce(sqlstate, ''), 2) = '40'
            THEN 'retryable_serialization'
        WHEN left(coalesce(sqlstate, ''), 2) IN ('08', '53', '57')
            THEN 'retryable_dependency'
        WHEN left(coalesce(sqlstate, ''), 2) IN ('22', '23', '42')
            THEN 'permanent_configuration'
        ELSE 'invariant_violation'
    END
$$;

CREATE FUNCTION memory.create_deletion_operation(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_principal_id text,
    candidate_idempotency_key text,
    candidate_request_fingerprint_sha256 character(64),
    candidate_configured_targets text[],
    candidate_retention_hours integer
)
RETURNS TABLE(
    operation_id uuid,
    lifecycle_state text,
    state_version bigint,
    replayed boolean,
    targets jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    existing_operation memory.deletion_operations%ROWTYPE;
    existing_tombstone memory.deletion_tombstones%ROWTYPE;
    existing_scope_subject_id uuid;
    existing_request_fingerprint character(64);
    candidate_scope_digest text;
    candidate_idempotency_key_digest character(64);
    target_name text;
    target_states jsonb;
BEGIN
    candidate_scope_digest := memory.deletion_scope_digest(
        candidate_tenant_id,
        candidate_subject_id
    );
    candidate_idempotency_key_digest := encode(
        sha256(convert_to(
            'palimpsest.deletion-idempotency/v1:'
            || candidate_principal_id || ':' || candidate_idempotency_key,
            'UTF8'
        )),
        'hex'
    );
    PERFORM set_config('palimpsest.scope_digest', candidate_scope_digest, true);

    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
       OR candidate_principal_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.principal_id', true), '') THEN
        RAISE EXCEPTION 'deletion operation scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            candidate_tenant_id::text || ':' || candidate_subject_id::text,
            0
        )
    );

    SELECT keys.subject_id, keys.request_fingerprint_sha256
    INTO existing_scope_subject_id, existing_request_fingerprint
    FROM memory.deletion_idempotency_keys AS keys
    WHERE keys.tenant_id = candidate_tenant_id
      AND keys.principal_id = candidate_principal_id
      AND keys.idempotency_key = candidate_idempotency_key;

    IF FOUND THEN
        IF existing_scope_subject_id IS DISTINCT FROM candidate_subject_id THEN
            RAISE EXCEPTION 'idempotency key was reused for a different subject'
                USING ERRCODE = 'P0004';
        END IF;
        SELECT operation.*
        INTO existing_operation
        FROM memory.deletion_operations AS operation
        JOIN memory.deletion_idempotency_keys AS keys
            ON keys.tenant_id = operation.tenant_id
           AND keys.subject_id = operation.subject_id
           AND keys.operation_id = operation.operation_id
        WHERE operation.tenant_id = candidate_tenant_id
          AND operation.subject_id = candidate_subject_id
          AND keys.principal_id = candidate_principal_id
          AND keys.idempotency_key = candidate_idempotency_key;
        IF existing_request_fingerprint IS DISTINCT FROM
                candidate_request_fingerprint_sha256 THEN
            RAISE EXCEPTION 'idempotency key was reused for a different request'
                USING ERRCODE = 'P0004';
        END IF;
        SELECT jsonb_agg(
            jsonb_build_object(
                'target_name', target.target_name,
                'target_key_digest', rtrim(target.target_key_digest::text),
                'capability', target.capability,
                'state', target.state,
                'attempts', target.attempts,
                'lease_id', target.lease_id,
                'lease_expires_at', target.lease_expires_at,
                'effect_receipt_sha256', rtrim(target.effect_receipt_sha256::text),
                'sanitized_error', target.sanitized_error
            ) ORDER BY target.target_name
        )
        INTO target_states
        FROM memory.deletion_targets AS target
        WHERE target.tenant_id = candidate_tenant_id
          AND target.subject_id = candidate_subject_id
          AND target.operation_id = existing_operation.operation_id;
        operation_id := existing_operation.operation_id;
        lifecycle_state := existing_operation.lifecycle_state;
        state_version := existing_operation.state_version;
        replayed := true;
        targets := target_states;
        RETURN NEXT;
        RETURN;
    END IF;

    SELECT tombstone.*
    INTO existing_tombstone
    FROM memory.deletion_tombstones AS tombstone
    WHERE tombstone.tenant_id = candidate_tenant_id
      AND tombstone.scope_digest = candidate_scope_digest
      AND tombstone.idempotency_key_digest = candidate_idempotency_key_digest;

    IF FOUND THEN
        IF existing_tombstone.request_fingerprint_sha256 IS DISTINCT FROM
                candidate_request_fingerprint_sha256 THEN
            RAISE EXCEPTION 'idempotency key was reused for a different request'
                USING ERRCODE = 'P0004';
        END IF;
        operation_id := existing_tombstone.operation_id;
        lifecycle_state := existing_tombstone.lifecycle_state;
        state_version := existing_tombstone.state_version;
        replayed := true;
        targets := existing_tombstone.target_summary;
        RETURN NEXT;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM memory.deletion_operations AS operation
        WHERE operation.tenant_id = candidate_tenant_id
          AND operation.subject_id = candidate_subject_id
          AND operation.lifecycle_state NOT IN ('completed', 'failed', 'expired')
    ) THEN
        RAISE EXCEPTION 'a deletion operation is already active for this subject'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO memory.deletion_operations (
        tenant_id,
        subject_id,
        operation_id,
        lifecycle_state,
        state_version,
        expires_at
    )
    VALUES (
        candidate_tenant_id,
        candidate_subject_id,
        candidate_operation_id,
        'draining',
        1,
        clock_timestamp() + make_interval(hours => candidate_retention_hours)
    );

    FOREACH target_name IN ARRAY ARRAY[
        'canonical', 'projections', 'caches', 'exports', 'artifacts'
    ] LOOP
        INSERT INTO memory.deletion_targets (
            tenant_id,
            subject_id,
            operation_id,
            target_name,
            target_key_digest,
            capability,
            state
        )
        VALUES (
            candidate_tenant_id,
            candidate_subject_id,
            candidate_operation_id,
            target_name,
            memory.deletion_target_key_digest(target_name),
            CASE
                WHEN candidate_configured_targets @> ARRAY[target_name]
                    THEN 'configured'
                ELSE 'not_configured'
            END,
            CASE
                WHEN candidate_configured_targets @> ARRAY[target_name]
                    THEN 'pending'
                ELSE 'not_configured'
            END
        );
    END LOOP;

    INSERT INTO memory.deletion_idempotency_keys (
        tenant_id,
        subject_id,
        principal_id,
        operation_id,
        idempotency_key,
        request_fingerprint_sha256
    )
    VALUES (
        candidate_tenant_id,
        candidate_subject_id,
        candidate_principal_id,
        candidate_operation_id,
        candidate_idempotency_key,
        candidate_request_fingerprint_sha256
    );

    INSERT INTO memory.outbox_intents (
        tenant_id,
        subject_id,
        case_id,
        event_type,
        resource_deletion_operation_id,
        payload
    )
    VALUES (
        candidate_tenant_id,
        candidate_subject_id,
        '00000000-0000-0000-0000-000000000000',
        'subject_deletion',
        candidate_operation_id,
        jsonb_build_object('operation_id', candidate_operation_id)
    );

    -- Keep the operation, target ledger, idempotency reservation, and worker
    -- intent insertable under the existing active-subject restrictive policy;
    -- commit the monotonic fence before this transaction can return.
    PERFORM memory.transition_subject_to_deletion_pending(
        candidate_tenant_id,
        candidate_subject_id
    );

    SELECT jsonb_agg(
        jsonb_build_object(
            'target_name', target.target_name,
            'target_key_digest', rtrim(target.target_key_digest::text),
            'capability', target.capability,
            'state', target.state,
            'attempts', target.attempts,
            'lease_id', target.lease_id,
            'lease_expires_at', target.lease_expires_at,
            'effect_receipt_sha256', rtrim(target.effect_receipt_sha256::text),
            'sanitized_error', target.sanitized_error
        ) ORDER BY target.target_name
    )
    INTO target_states
    FROM memory.deletion_targets AS target
    WHERE target.tenant_id = candidate_tenant_id
      AND target.subject_id = candidate_subject_id
      AND target.operation_id = candidate_operation_id;

    operation_id := candidate_operation_id;
    lifecycle_state := 'draining';
    state_version := 1;
    replayed := false;
    targets := target_states;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.create_deletion_operation(
    uuid, uuid, uuid, text, text, character, text[], integer
)
FROM PUBLIC;

CREATE FUNCTION memory.poll_deletion_operation(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid
)
RETURNS TABLE(
    lifecycle_state text,
    state_version bigint,
    retry_count integer,
    failure_reason text,
    targets jsonb,
    updated_at timestamptz,
    expired boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate_scope_digest text;
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion operation scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    candidate_scope_digest := memory.deletion_scope_digest(
        candidate_tenant_id,
        candidate_subject_id
    );
    PERFORM set_config('palimpsest.scope_digest', candidate_scope_digest, true);

    RETURN QUERY
    SELECT
        operation.lifecycle_state,
        operation.state_version,
        operation.retry_count,
        operation.failure_reason,
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'target_name', target.target_name,
                    'target_key_digest', rtrim(target.target_key_digest::text),
                    'capability', target.capability,
                    'state', target.state,
                    'attempts', target.attempts,
                    'lease_id', target.lease_id,
                    'lease_expires_at', target.lease_expires_at,
                    'effect_receipt_sha256', rtrim(target.effect_receipt_sha256::text),
                    'sanitized_error', target.sanitized_error
                ) ORDER BY target.target_name
            )
            FROM memory.deletion_targets AS target
            WHERE target.tenant_id = candidate_tenant_id
              AND target.subject_id = candidate_subject_id
              AND target.operation_id = candidate_operation_id
        ),
        operation.updated_at,
        operation.expires_at <= clock_timestamp()
    FROM memory.deletion_operations AS operation
    WHERE operation.tenant_id = candidate_tenant_id
      AND operation.subject_id = candidate_subject_id
      AND operation.operation_id = candidate_operation_id;

    IF FOUND THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT tombstone.lifecycle_state,
           tombstone.state_version,
           0,
           NULL::text,
           tombstone.target_summary,
           tombstone.completed_at,
           tombstone.expires_at <= clock_timestamp()
    FROM memory.deletion_tombstones AS tombstone
    WHERE tombstone.tenant_id = candidate_tenant_id
      AND tombstone.scope_digest = candidate_scope_digest
      AND tombstone.operation_id = candidate_operation_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation does not exist'
            USING ERRCODE = 'P0002';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION memory.poll_deletion_operation(uuid, uuid, uuid)
FROM PUBLIC;

CREATE FUNCTION memory.repair_deletion_operation(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_reason_code text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    operation memory.deletion_operations%ROWTYPE;
    lifecycle text;
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion repair scope is not authorized'
            USING ERRCODE = '42501';
    END IF;
    IF candidate_reason_code IS NULL
       OR candidate_reason_code !~ '^[a-z][a-z0-9_]{0,63}$' THEN
        RAISE EXCEPTION 'deletion repair reason is invalid'
            USING ERRCODE = '22023';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            candidate_tenant_id::text || ':' || candidate_subject_id::text,
            0
        )
    );

    SELECT operation_row.*
    INTO operation
    FROM memory.deletion_operations AS operation_row
    WHERE operation_row.tenant_id = candidate_tenant_id
      AND operation_row.subject_id = candidate_subject_id
      AND operation_row.operation_id = candidate_operation_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation does not exist'
            USING ERRCODE = 'P0002';
    END IF;
    IF operation.lifecycle_state <> 'failed' THEN
        RAISE EXCEPTION 'only failed deletion operations can be repaired'
            USING ERRCODE = '55000';
    END IF;

    SELECT subject_lifecycles.lifecycle_state
    INTO lifecycle
    FROM memory.subject_lifecycles
    WHERE subject_lifecycles.tenant_id = candidate_tenant_id
      AND subject_lifecycles.subject_id = candidate_subject_id;
    IF lifecycle IS DISTINCT FROM 'deletion_pending' THEN
        RAISE EXCEPTION 'failed deletion repair requires a fenced subject'
            USING ERRCODE = '55000';
    END IF;

    UPDATE memory.deletion_targets AS target
    SET state = 'pending',
        lease_id = NULL,
        lease_expires_at = NULL,
        effect_receipt_sha256 = NULL,
        sanitized_error = NULL,
        updated_at = clock_timestamp()
    WHERE target.tenant_id = candidate_tenant_id
      AND target.subject_id = candidate_subject_id
      AND target.operation_id = candidate_operation_id
      AND target.capability = 'configured'
      AND target.state IN ('failed', 'leased');

    UPDATE memory.deletion_operations AS operation_row
    SET lifecycle_state = 'retry_wait',
        state_version = operation_row.state_version + 1,
        retry_count = 0,
        retry_at = clock_timestamp(),
        failure_reason = NULL,
        worker_lease_id = NULL,
        worker_lease_expires_at = NULL,
        completed_at = NULL,
        repair_count = operation_row.repair_count + 1,
        last_repair_reason = candidate_reason_code,
        last_repaired_at = clock_timestamp(),
        updated_at = clock_timestamp()
    WHERE operation_row.tenant_id = candidate_tenant_id
      AND operation_row.subject_id = candidate_subject_id
      AND operation_row.operation_id = candidate_operation_id;
END;
$$;

REVOKE ALL ON FUNCTION memory.repair_deletion_operation(uuid, uuid, uuid, text)
FROM PUBLIC;

CREATE FUNCTION memory.claim_next_deletion_operation(
    candidate_worker_id uuid,
    candidate_lease_seconds integer
)
RETURNS TABLE(
    tenant_id uuid,
    subject_id uuid,
    operation_id uuid,
    lifecycle_state text,
    state_version bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.deletion_operations%ROWTYPE;
BEGIN
    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    SELECT operation.*
    INTO candidate
    FROM memory.deletion_operations AS operation
    WHERE operation.lifecycle_state IN (
            'draining', 'fenced', 'purging', 'retry_wait'
        )
      AND (
          operation.worker_lease_id IS NULL
          OR operation.worker_lease_expires_at <= clock_timestamp()
      )
      AND (
          operation.lifecycle_state <> 'retry_wait'
          OR operation.retry_at IS NULL
          OR operation.retry_at <= clock_timestamp()
      )
    ORDER BY operation.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.deletion_operations AS operation_row
    SET worker_lease_id = candidate_worker_id,
        worker_lease_expires_at =
            clock_timestamp() + make_interval(secs => candidate_lease_seconds)
    WHERE operation_row.tenant_id = candidate.tenant_id
      AND operation_row.subject_id = candidate.subject_id
      AND operation_row.operation_id = candidate.operation_id;

    tenant_id := candidate.tenant_id;
    subject_id := candidate.subject_id;
    operation_id := candidate.operation_id;
    lifecycle_state := candidate.lifecycle_state;
    state_version := candidate.state_version;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_deletion_operation(uuid, integer)
FROM PUBLIC;

CREATE FUNCTION memory.claim_next_deletion_target(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_target_lease_id uuid,
    candidate_lease_seconds integer
)
RETURNS TABLE(
    target_name text,
    target_key_digest character(64),
    target_lease_id uuid,
    attempts integer,
    lease_expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    operation memory.deletion_operations%ROWTYPE;
    target memory.deletion_targets%ROWTYPE;
BEGIN
    IF candidate_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'deletion target lease must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion target scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    SELECT operation_row.*
    INTO operation
    FROM memory.deletion_operations AS operation_row
    WHERE operation_row.tenant_id = candidate_tenant_id
      AND operation_row.subject_id = candidate_subject_id
      AND operation_row.operation_id = candidate_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation does not exist'
            USING ERRCODE = 'P0002';
    END IF;
    IF operation.lifecycle_state <> 'purging'
       OR operation.worker_lease_id IS DISTINCT FROM candidate_worker_id
       OR operation.worker_lease_expires_at IS NULL
       OR operation.worker_lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'deletion operation worker lease is not held'
            USING ERRCODE = '55000';
    END IF;

    SELECT target_row.*
    INTO target
    FROM memory.deletion_targets AS target_row
    WHERE target_row.tenant_id = candidate_tenant_id
      AND target_row.subject_id = candidate_subject_id
      AND target_row.operation_id = candidate_operation_id
      AND target_row.capability = 'configured'
      AND (
          target_row.state = 'pending'
          OR (
              target_row.state = 'leased'
              AND target_row.lease_expires_at IS NOT NULL
              AND target_row.lease_expires_at <= clock_timestamp()
          )
      )
    ORDER BY CASE target_row.target_name
        WHEN 'projections' THEN 1
        WHEN 'caches' THEN 2
        WHEN 'exports' THEN 3
        WHEN 'artifacts' THEN 4
        WHEN 'canonical' THEN 5
    END, target_row.target_key_digest
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.deletion_targets AS target_row
    SET state = 'leased',
        lease_id = candidate_target_lease_id,
        lease_expires_at = clock_timestamp()
            + make_interval(secs => candidate_lease_seconds),
        attempts = target.attempts + 1,
        sanitized_error = NULL,
        updated_at = clock_timestamp()
    WHERE target_row.tenant_id = candidate_tenant_id
      AND target_row.subject_id = candidate_subject_id
      AND target_row.operation_id = candidate_operation_id
      AND target_row.target_name = target.target_name
      AND target_row.target_key_digest = target.target_key_digest
    RETURNING target_row.lease_expires_at INTO lease_expires_at;

    target_name := target.target_name;
    target_key_digest := target.target_key_digest;
    target_lease_id := candidate_target_lease_id;
    attempts := target.attempts + 1;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_deletion_target(
    uuid, uuid, uuid, uuid, uuid, integer
)
FROM PUBLIC;

CREATE FUNCTION memory.complete_deletion_target(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_target_name text,
    candidate_target_key_digest character(64),
    candidate_target_lease_id uuid,
    candidate_effect_receipt_sha256 character(64)
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion target scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    UPDATE memory.deletion_targets AS target
    SET state = 'done',
        lease_id = NULL,
        lease_expires_at = NULL,
        effect_receipt_sha256 = candidate_effect_receipt_sha256,
        sanitized_error = NULL,
        updated_at = clock_timestamp()
    FROM memory.deletion_operations AS operation
    WHERE target.tenant_id = candidate_tenant_id
      AND target.subject_id = candidate_subject_id
      AND target.operation_id = candidate_operation_id
      AND target.target_name = candidate_target_name
      AND target.target_key_digest = candidate_target_key_digest
      AND target.state = 'leased'
      AND target.lease_id = candidate_target_lease_id
      AND operation.tenant_id = target.tenant_id
      AND operation.subject_id = target.subject_id
      AND operation.operation_id = target.operation_id
      AND operation.lifecycle_state = 'purging'
      AND operation.worker_lease_id = candidate_worker_id
      AND operation.worker_lease_expires_at > clock_timestamp();

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion target lease is not held'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION memory.complete_deletion_target(
    uuid, uuid, uuid, uuid, text, character, uuid, character
)
FROM PUBLIC;

CREATE FUNCTION memory.fail_deletion_target(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_target_name text,
    candidate_target_key_digest character(64),
    candidate_target_lease_id uuid,
    candidate_sanitized_error text,
    candidate_max_attempts integer
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    next_state text;
BEGIN
    IF candidate_max_attempts <= 0 THEN
        RAISE EXCEPTION 'deletion target max attempts must be positive'
            USING ERRCODE = '22023';
    END IF;
    IF candidate_sanitized_error IS NULL
       OR length(candidate_sanitized_error) > 200
       OR candidate_sanitized_error !~ '^[a-z0-9 ._-]+$' THEN
        RAISE EXCEPTION 'deletion target error is not sanitized'
            USING ERRCODE = '22023';
    END IF;
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion target scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    UPDATE memory.deletion_targets AS target
    SET state = CASE
            WHEN target.attempts >= candidate_max_attempts THEN 'failed'
            ELSE 'pending'
        END,
        lease_id = NULL,
        lease_expires_at = NULL,
        sanitized_error = candidate_sanitized_error,
        updated_at = clock_timestamp()
    FROM memory.deletion_operations AS operation
    WHERE target.tenant_id = candidate_tenant_id
      AND target.subject_id = candidate_subject_id
      AND target.operation_id = candidate_operation_id
      AND target.target_name = candidate_target_name
      AND target.target_key_digest = candidate_target_key_digest
      AND target.state = 'leased'
      AND target.lease_id = candidate_target_lease_id
      AND operation.tenant_id = target.tenant_id
      AND operation.subject_id = target.subject_id
      AND operation.operation_id = target.operation_id
      AND operation.lifecycle_state = 'purging'
      AND operation.worker_lease_id = candidate_worker_id
      AND operation.worker_lease_expires_at > clock_timestamp()
    RETURNING target.state INTO next_state;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion target lease is not held'
            USING ERRCODE = '55000';
    END IF;
    RETURN next_state;
END;
$$;

REVOKE ALL ON FUNCTION memory.fail_deletion_target(
    uuid, uuid, uuid, uuid, text, character, uuid, text, integer
)
FROM PUBLIC;

CREATE FUNCTION memory.purge_deletion_target(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_target_name text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion purge scope is not authorized'
            USING ERRCODE = '42501';
    END IF;
    IF NULLIF(current_setting('palimpsest.deletion_workflow', true), '') IS NULL THEN
        RAISE EXCEPTION 'deletion purge requires the deletion workflow marker'
            USING ERRCODE = '42501';
    END IF;

    IF candidate_target_name = 'projections' THEN
        DELETE FROM memory.retrieval_manifest_items
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.retrieval_receipts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.retrieval_idempotency_reservations
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_search_documents
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
    ELSIF candidate_target_name IN ('caches', 'artifacts') THEN
        -- These optional providers are not configured by the PostgreSQL
        -- adapter; the ledger records their verified absence.
        NULL;
    ELSIF candidate_target_name = 'exports' THEN
        -- The export schema is introduced after this migration. Keep the
        -- reference dynamic so a fresh migration run remains valid while the
        -- later export target still receives a real purge.
        IF to_regclass('memory.export_manifest_items') IS NOT NULL THEN
            EXECUTE
                'DELETE FROM memory.export_manifest_items
                 WHERE tenant_id = $1 AND subject_id = $2'
            USING candidate_tenant_id, candidate_subject_id;
            EXECUTE
                'DELETE FROM memory.export_operations
                 WHERE tenant_id = $1 AND subject_id = $2'
            USING candidate_tenant_id, candidate_subject_id;
        END IF;
    ELSIF candidate_target_name = 'canonical' THEN
        DELETE FROM memory.retrieval_manifest_items
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.retrieval_receipts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.retrieval_idempotency_reservations
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_evidence
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_governance
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_search_documents
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.outbox_intents
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.idempotency_receipts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.write_audit_receipts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.checkpoint_effect_receipts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.checkpoint_effect_intents
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.checkpoint_revisions
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.checkpoints
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.fact_revisions
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.facts
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.episodes
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
        DELETE FROM memory.subject_content_leases
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id;
    ELSE
        RAISE EXCEPTION 'unknown deletion target %', candidate_target_name
            USING ERRCODE = '22023';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION memory.purge_deletion_target(uuid, uuid, text)
FROM PUBLIC;

CREATE FUNCTION memory.verify_deletion_operation(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    lifecycle text;
    residual bigint := 0;
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion verification scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    SELECT subject_lifecycles.lifecycle_state
    INTO lifecycle
    FROM memory.subject_lifecycles
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;

    IF NOT FOUND OR lifecycle <> 'deletion_pending' THEN
        RAISE EXCEPTION 'deletion verification requires a fenced subject'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT
        (SELECT count(*) FROM memory.episodes
         WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.facts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revisions
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_evidence
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_governance
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_search_documents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_embedding_projections
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoints
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_revisions
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_effect_intents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_effect_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.outbox_intents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.idempotency_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.write_audit_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_manifest_items
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_idempotency_reservations
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + CASE
            WHEN to_regclass('memory.export_manifest_items') IS NOT NULL THEN
                (SELECT count(*) FROM memory.export_manifest_items
                 WHERE tenant_id = candidate_tenant_id
                   AND subject_id = candidate_subject_id)
                + (SELECT count(*) FROM memory.export_operations
                   WHERE tenant_id = candidate_tenant_id
                     AND subject_id = candidate_subject_id)
            ELSE 0
          END
        + (SELECT count(*) FROM memory.subject_content_leases
           WHERE tenant_id = candidate_tenant_id
             AND subject_id = candidate_subject_id
             AND expires_at > clock_timestamp())
    INTO residual;

    IF residual > 0 THEN
        RAISE EXCEPTION 'deletion verification found % residual rows', residual
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION memory.verify_deletion_operation(uuid, uuid)
FROM PUBLIC;

CREATE FUNCTION memory.advance_deletion_operation(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid,
    candidate_max_attempts integer
)
RETURNS TABLE(
    lifecycle_state text,
    state_version bigint,
    next_poll_seconds integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    operation memory.deletion_operations%ROWTYPE;
    current_version bigint;
    current_retry integer;
    backoff_seconds integer;
    candidate_scope_digest text;
    candidate_idempotency_key_digest character(64);
    request_fingerprint character(64);
    target_summary jsonb;
    verification_digest character(64);
BEGIN
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion operation scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            candidate_tenant_id::text || ':' || candidate_subject_id::text,
            0
        )
    );

    SELECT operation_row.*
    INTO operation
    FROM memory.deletion_operations AS operation_row
    WHERE operation_row.tenant_id = candidate_tenant_id
      AND operation_row.subject_id = candidate_subject_id
      AND operation_row.operation_id = candidate_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation does not exist'
            USING ERRCODE = 'P0002';
    END IF;
    IF operation.worker_lease_id IS DISTINCT FROM candidate_worker_id
       OR operation.worker_lease_expires_at IS NULL
       OR operation.worker_lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'deletion operation worker lease is not held'
            USING ERRCODE = '55000';
    END IF;

    current_version := operation.state_version;

    IF operation.lifecycle_state = 'retry_wait' THEN
        UPDATE memory.deletion_operations AS operation_row
        SET lifecycle_state = 'purging',
            state_version = operation_row.state_version + 1
        WHERE operation_row.tenant_id = candidate_tenant_id
          AND operation_row.subject_id = candidate_subject_id
          AND operation_row.operation_id = candidate_operation_id
        RETURNING operation_row.state_version INTO current_version;
        operation.lifecycle_state := 'purging';
    END IF;

    IF operation.lifecycle_state = 'draining' THEN
        DELETE FROM memory.subject_content_leases AS lease
        WHERE lease.tenant_id = candidate_tenant_id
          AND lease.subject_id = candidate_subject_id
          AND lease.expires_at <= clock_timestamp();
        IF EXISTS (
            SELECT 1
            FROM memory.subject_content_leases AS lease
            WHERE lease.tenant_id = candidate_tenant_id
              AND lease.subject_id = candidate_subject_id
              AND lease.expires_at > clock_timestamp()
        ) THEN
            UPDATE memory.deletion_operations AS operation_row
            SET worker_lease_id = NULL,
                worker_lease_expires_at = NULL
            WHERE operation_row.tenant_id = candidate_tenant_id
              AND operation_row.subject_id = candidate_subject_id
              AND operation_row.operation_id = candidate_operation_id;
            RETURN QUERY SELECT operation.lifecycle_state, current_version, 5;
            RETURN;
        END IF;
        UPDATE memory.deletion_operations AS operation_row
        SET lifecycle_state = 'fenced',
            state_version = operation_row.state_version + 1
        WHERE operation_row.tenant_id = candidate_tenant_id
          AND operation_row.subject_id = candidate_subject_id
          AND operation_row.operation_id = candidate_operation_id
        RETURNING operation_row.state_version INTO current_version;
        operation.lifecycle_state := 'fenced';
    END IF;

    IF operation.lifecycle_state = 'fenced' THEN
        UPDATE memory.deletion_operations AS operation_row
        SET lifecycle_state = 'purging',
            state_version = operation_row.state_version + 1
        WHERE operation_row.tenant_id = candidate_tenant_id
          AND operation_row.subject_id = candidate_subject_id
          AND operation_row.operation_id = candidate_operation_id
        RETURNING operation_row.state_version INTO current_version;
        operation.lifecycle_state := 'purging';
    END IF;

    IF operation.lifecycle_state = 'purging' THEN
        IF EXISTS (
            SELECT 1
            FROM memory.deletion_targets AS target_row
            WHERE target_row.tenant_id = candidate_tenant_id
              AND target_row.subject_id = candidate_subject_id
              AND target_row.operation_id = candidate_operation_id
              AND target_row.capability = 'configured'
              AND target_row.state = 'failed'
        ) THEN
            UPDATE memory.deletion_operations AS operation_row
            SET lifecycle_state = 'failed',
                state_version = operation_row.state_version + 1,
                failure_reason = 'deletion target exceeded retry limit',
                completed_at = clock_timestamp(),
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL
            WHERE operation_row.tenant_id = candidate_tenant_id
              AND operation_row.subject_id = candidate_subject_id
              AND operation_row.operation_id = candidate_operation_id
            RETURNING operation_row.state_version INTO current_version;
            RETURN QUERY SELECT 'failed', current_version, 0;
            RETURN;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM memory.deletion_targets AS target_row
            WHERE target_row.tenant_id = candidate_tenant_id
              AND target_row.subject_id = candidate_subject_id
              AND target_row.operation_id = candidate_operation_id
              AND target_row.capability = 'configured'
              AND (
                  target_row.state = 'pending'
                  OR (
                      target_row.state = 'leased'
                      AND target_row.lease_expires_at IS NOT NULL
                      AND target_row.lease_expires_at <= clock_timestamp()
                  )
              )
        ) THEN
            RETURN QUERY SELECT 'purging', current_version, 0;
            RETURN;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM memory.deletion_targets AS target_row
            WHERE target_row.tenant_id = candidate_tenant_id
              AND target_row.subject_id = candidate_subject_id
              AND target_row.operation_id = candidate_operation_id
              AND target_row.capability = 'configured'
              AND target_row.state = 'leased'
        ) THEN
            RETURN QUERY SELECT 'purging', current_version, 1;
            RETURN;
        END IF;

        UPDATE memory.deletion_operations AS operation_row
        SET lifecycle_state = 'verifying',
            state_version = operation_row.state_version + 1
        WHERE operation_row.tenant_id = candidate_tenant_id
          AND operation_row.subject_id = candidate_subject_id
          AND operation_row.operation_id = candidate_operation_id
        RETURNING operation_row.state_version INTO current_version;
        operation.lifecycle_state := 'verifying';
    END IF;

    IF operation.lifecycle_state = 'verifying' THEN
        BEGIN
            PERFORM set_config(
                'palimpsest.deletion_workflow',
                candidate_operation_id::text,
                true
            );
            PERFORM memory.verify_deletion_operation(
                candidate_tenant_id,
                candidate_subject_id
            );
        EXCEPTION WHEN OTHERS THEN
            backoff_seconds := least(
                (2::double precision ^ (operation.retry_count + 1))::integer,
                300
            );
            UPDATE memory.deletion_operations AS operation_row
            SET lifecycle_state = 'retry_wait',
                state_version = operation_row.state_version + 1,
                retry_count = operation_row.retry_count + 1,
                retry_at = clock_timestamp()
                    + make_interval(secs => backoff_seconds),
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL
            WHERE operation_row.tenant_id = candidate_tenant_id
              AND operation_row.subject_id = candidate_subject_id
              AND operation_row.operation_id = candidate_operation_id
            RETURNING operation_row.state_version, operation_row.retry_count
                INTO current_version, current_retry;
            IF current_retry >= candidate_max_attempts THEN
                UPDATE memory.deletion_operations AS operation_row
                SET lifecycle_state = 'failed',
                    state_version = operation_row.state_version + 1,
                    failure_reason = 'deletion verification exceeded '
                        || candidate_max_attempts || ' attempts',
                    completed_at = clock_timestamp(),
                    worker_lease_id = NULL,
                    worker_lease_expires_at = NULL
                WHERE operation_row.tenant_id = candidate_tenant_id
                  AND operation_row.subject_id = candidate_subject_id
                  AND operation_row.operation_id = candidate_operation_id
                RETURNING operation_row.state_version INTO current_version;
                RETURN QUERY SELECT 'failed', current_version, 0;
            ELSE
                RETURN QUERY SELECT 'retry_wait', current_version, backoff_seconds;
            END IF;
            RETURN;
        END;

        candidate_scope_digest := memory.deletion_scope_digest(
            candidate_tenant_id,
            candidate_subject_id
        );
        PERFORM set_config('palimpsest.scope_digest', candidate_scope_digest, true);
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'target_name', target.target_name,
                    'target_key_digest', rtrim(target.target_key_digest::text),
                    'capability', target.capability,
                    'state', target.state,
                    'attempts', target.attempts,
                    'effect_receipt_sha256', rtrim(target.effect_receipt_sha256::text),
                    'sanitized_error', target.sanitized_error
                ) ORDER BY target.target_name
            ),
            '[]'::jsonb
        )
        INTO target_summary
        FROM memory.deletion_targets AS target
        WHERE target.tenant_id = candidate_tenant_id
          AND target.subject_id = candidate_subject_id
          AND target.operation_id = candidate_operation_id;
        verification_digest := encode(
            sha256(convert_to(target_summary::text, 'UTF8')),
            'hex'
        );
        SELECT encode(
                   sha256(convert_to(
                       'palimpsest.deletion-idempotency/v1:'
                       || keys.principal_id || ':' || keys.idempotency_key,
                       'UTF8'
                   )),
                   'hex'
               ),
               keys.request_fingerprint_sha256
        INTO candidate_idempotency_key_digest, request_fingerprint
        FROM memory.deletion_idempotency_keys AS keys
        WHERE keys.tenant_id = candidate_tenant_id
          AND keys.subject_id = candidate_subject_id
          AND keys.operation_id = candidate_operation_id
        LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'deletion idempotency evidence is missing'
                USING ERRCODE = 'P0001';
        END IF;
        INSERT INTO memory.deletion_tombstones (
            tenant_id,
            scope_digest,
            operation_id,
            lifecycle_state,
            state_version,
            idempotency_key_digest,
            request_fingerprint_sha256,
            policy_version,
            contract_schema_version,
            worker_release,
            target_summary,
            verification_digest,
            backup_policy_id,
            expires_at
        )
        VALUES (
            candidate_tenant_id,
            candidate_scope_digest,
            candidate_operation_id,
            'completed',
            operation.state_version + 1,
            candidate_idempotency_key_digest,
            request_fingerprint,
            'subject-delete/v1',
            1,
            'palimpsest-deletion-worker/v1',
            target_summary,
            verification_digest,
            'isolated-until-expiry/operator-declared',
            operation.expires_at
        );
        DELETE FROM memory.outbox_intents
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
          AND resource_deletion_operation_id = candidate_operation_id;
        DELETE FROM memory.deletion_targets
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
          AND operation_id = candidate_operation_id;
        DELETE FROM memory.deletion_idempotency_keys
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
          AND operation_id = candidate_operation_id;
        DELETE FROM memory.deletion_operations
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
          AND operation_id = candidate_operation_id;
        PERFORM memory.transition_subject_to_deleted(
            candidate_tenant_id,
            candidate_subject_id
        );
        RETURN QUERY SELECT 'completed', operation.state_version + 1, 0;
        RETURN;
    END IF;

    UPDATE memory.deletion_operations AS operation_row
    SET worker_lease_id = NULL,
        worker_lease_expires_at = NULL
    WHERE operation_row.tenant_id = candidate_tenant_id
      AND operation_row.subject_id = candidate_subject_id
      AND operation_row.operation_id = candidate_operation_id;
    RETURN QUERY SELECT operation.lifecycle_state, current_version, 0;
END;
$$;

REVOKE ALL ON FUNCTION memory.advance_deletion_operation(
    uuid, uuid, uuid, uuid, integer
)
FROM PUBLIC;
