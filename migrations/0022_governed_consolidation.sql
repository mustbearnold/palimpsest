-- 0022: governed consolidation (spec 011)
-- Durable consolidation jobs and claims with an attributable interpreter
-- boundary. Mirrors the deletion worker patterns (0010): claim-level leases,
-- worker-claim RLS escape, sanitized failure reasons, content-free rows.

CREATE TABLE memory.consolidation_interpreter_configs (
    tenant_id uuid NOT NULL,
    interpreter_config_id uuid NOT NULL,
    provider_kind text NOT NULL CHECK (
        provider_kind IN ('fixture-deterministic-v1')
    ),
    prompt_policy_version text NOT NULL CHECK (
        char_length(prompt_policy_version) BETWEEN 1 AND 128
    ),
    config_digest character(64) NOT NULL CHECK (
        config_digest ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_by_principal_id text NOT NULL CHECK (
        char_length(created_by_principal_id) BETWEEN 1 AND 255
    ),
    PRIMARY KEY (tenant_id, interpreter_config_id)
);

CREATE TABLE memory.consolidation_policies (
    tenant_id uuid NOT NULL,
    source_kind text NOT NULL CHECK (
        char_length(source_kind) BETWEEN 1 AND 128
    ),
    policy_id text NOT NULL CHECK (
        char_length(policy_id) BETWEEN 1 AND 128
    ),
    interpreter_config_id uuid NOT NULL,
    write_policy_id text NOT NULL CHECK (
        char_length(write_policy_id) BETWEEN 1 AND 128
    ),
    write_policy_version text NOT NULL CHECK (
        char_length(write_policy_version) BETWEEN 1 AND 128
    ),
    retention_policy_id text NOT NULL CHECK (
        char_length(retention_policy_id) BETWEEN 1 AND 128
    ),
    confidence_auto_promote_min double precision NOT NULL CHECK (
        confidence_auto_promote_min >= 0.0
        AND confidence_auto_promote_min <= 1.0
    ),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_by_principal_id text NOT NULL CHECK (
        char_length(created_by_principal_id) BETWEEN 1 AND 255
    ),
    PRIMARY KEY (tenant_id, source_kind, policy_id),
    FOREIGN KEY (tenant_id, interpreter_config_id)
        REFERENCES memory.consolidation_interpreter_configs
            (tenant_id, interpreter_config_id)
        ON DELETE RESTRICT
);

CREATE TABLE memory.consolidation_jobs (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    job_id uuid NOT NULL,
    source_kind text NOT NULL CHECK (
        char_length(source_kind) BETWEEN 1 AND 128
    ),
    policy_id text NOT NULL CHECK (
        char_length(policy_id) BETWEEN 1 AND 128
    ),
    policy_version text NOT NULL CHECK (
        char_length(policy_version) BETWEEN 1 AND 128
    ),
    window_from timestamptz NOT NULL CHECK (isfinite(window_from)),
    window_until timestamptz NOT NULL CHECK (
        isfinite(window_until) AND window_until > window_from
    ),
    lifecycle_state text NOT NULL DEFAULT 'pending' CHECK (
        lifecycle_state IN ('pending', 'running', 'complete', 'failed')
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
    claim_cap integer NOT NULL CHECK (claim_cap BETWEEN 1 AND 100000),
    claims_total integer NOT NULL DEFAULT 0 CHECK (
        claims_total >= 0 AND claims_total <= claim_cap
    ),
    claims_done integer NOT NULL DEFAULT 0 CHECK (
        claims_done >= 0 AND claims_done <= claims_total
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    principal_id text NOT NULL CHECK (
        char_length(principal_id) BETWEEN 1 AND 255
    ),
    idempotency_key_digest character(64) NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    PRIMARY KEY (tenant_id, subject_id, job_id),
    CHECK (completed_at IS NULL OR lifecycle_state = 'complete'),
    CHECK (lifecycle_state <> 'complete' OR completed_at IS NOT NULL)
);

CREATE UNIQUE INDEX consolidation_jobs_idempotency_key_idx
    ON memory.consolidation_jobs (
        tenant_id, subject_id, principal_id, idempotency_key_digest
    );

CREATE TABLE memory.consolidation_claims (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    job_id uuid NOT NULL,
    claim_id uuid NOT NULL,
    episode_ids uuid[] NOT NULL CHECK (
        cardinality(episode_ids) BETWEEN 1 AND 1024
    ),
    content_hash character(64) NOT NULL CHECK (
        content_hash ~ '^[0-9a-f]{64}$'
    ),
    confidence double precision NOT NULL CHECK (
        confidence >= 0.0 AND confidence <= 1.0
    ),
    sensitivity text NOT NULL CHECK (
        btrim(sensitivity) <> '' AND length(sensitivity) <= 255
    ),
    valid_from timestamptz NOT NULL CHECK (isfinite(valid_from)),
    valid_until timestamptz CHECK (
        valid_until IS NULL
        OR (isfinite(valid_until) AND valid_until > valid_from)
    ),
    observed_at timestamptz NOT NULL CHECK (isfinite(observed_at)),
    value jsonb NOT NULL,
    model_identity text NOT NULL CHECK (
        char_length(model_identity) BETWEEN 1 AND 255
    ),
    prompt_policy_version text NOT NULL CHECK (
        char_length(prompt_policy_version) BETWEEN 1 AND 128
    ),
    lifecycle_state text NOT NULL DEFAULT 'pending' CHECK (
        lifecycle_state IN ('pending', 'leased', 'done', 'skipped')
    ),
    skip_reason text CHECK (
        skip_reason IS NULL OR skip_reason IN ('low_confidence', 'materialization_failed')
    ),
    fact_id uuid,
    revision_id uuid,
    lease_id uuid,
    lease_expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id, job_id, claim_id),
    FOREIGN KEY (tenant_id, subject_id, job_id)
        REFERENCES memory.consolidation_jobs (tenant_id, subject_id, job_id)
        ON DELETE RESTRICT,
    CHECK (lifecycle_state IN ('done', 'skipped') OR fact_id IS NULL),
    CHECK (lifecycle_state <> 'done' OR fact_id IS NOT NULL)
);

CREATE INDEX consolidation_claims_claimable_idx
    ON memory.consolidation_claims (tenant_id, subject_id, job_id)
    WHERE lifecycle_state = 'pending';

-- Worker claim escape. Mirrors 0010: a trusted server-side worker sets
-- palimpsest.worker_claim before it reads or writes job and claim rows.
CREATE FUNCTION memory.set_worker_claim_context()
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
    SELECT set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
$$;

REVOKE ALL ON FUNCTION memory.set_worker_claim_context() FROM PUBLIC;

CREATE FUNCTION memory.claim_next_consolidation_job(
    candidate_worker_id uuid,
    candidate_lease_seconds integer
)
RETURNS TABLE(
    tenant_id uuid,
    subject_id uuid,
    job_id uuid,
    source_kind text,
    policy_id text,
    policy_version text,
    window_from timestamptz,
    window_until timestamptz,
    claim_cap integer,
    principal_id text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.consolidation_jobs%ROWTYPE;
BEGIN
    PERFORM memory.set_worker_claim_context();

    SELECT job.*
    INTO candidate
    FROM memory.consolidation_jobs AS job
    WHERE (
            job.lifecycle_state = 'pending'
            OR (
                job.lifecycle_state = 'running'
                AND job.worker_lease_expires_at <= clock_timestamp()
            )
        )
      AND (
          job.worker_lease_id IS NULL
          OR job.worker_lease_expires_at <= clock_timestamp()
      )
    ORDER BY job.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.consolidation_jobs AS job_row
    SET lifecycle_state = 'running',
        state_version = job_row.state_version + 1,
        worker_lease_id = candidate_worker_id,
        worker_lease_expires_at =
            clock_timestamp() + make_interval(secs => candidate_lease_seconds),
        updated_at = clock_timestamp()
    WHERE job_row.tenant_id = candidate.tenant_id
      AND job_row.subject_id = candidate.subject_id
      AND job_row.job_id = candidate.job_id;

    tenant_id := candidate.tenant_id;
    subject_id := candidate.subject_id;
    job_id := candidate.job_id;
    source_kind := candidate.source_kind;
    policy_id := candidate.policy_id;
    policy_version := candidate.policy_version;
    window_from := candidate.window_from;
    window_until := candidate.window_until;
    claim_cap := candidate.claim_cap;
    principal_id := candidate.principal_id;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_consolidation_job(uuid, integer)
FROM PUBLIC;

CREATE FUNCTION memory.claim_next_consolidation_claim(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid,
    candidate_worker_id uuid,
    candidate_lease_seconds integer
)
RETURNS TABLE(
    claim_id uuid,
    case_id uuid,
    episode_ids uuid[],
    content_hash character(64),
    confidence double precision,
    sensitivity text,
    valid_from timestamptz,
    valid_until timestamptz,
    observed_at timestamptz,
    value jsonb,
    model_identity text,
    prompt_policy_version text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.consolidation_claims%ROWTYPE;
BEGIN
    PERFORM memory.set_worker_claim_context();

    SELECT claim.*
    INTO candidate
    FROM memory.consolidation_claims AS claim
    WHERE claim.tenant_id = candidate_tenant_id
      AND claim.subject_id = candidate_subject_id
      AND claim.job_id = candidate_job_id
      AND (
          claim.lifecycle_state = 'pending'
          OR (
              claim.lifecycle_state = 'leased'
              AND claim.lease_expires_at <= clock_timestamp()
          )
      )
      AND (
          claim.lease_id IS NULL
          OR claim.lease_expires_at <= clock_timestamp()
      )
    ORDER BY claim.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.consolidation_claims AS claim_row
    SET lifecycle_state = 'leased',
        lease_id = candidate_worker_id,
        lease_expires_at =
            clock_timestamp() + make_interval(secs => candidate_lease_seconds),
        attempts = claim_row.attempts + 1,
        updated_at = clock_timestamp()
    WHERE claim_row.tenant_id = candidate.tenant_id
      AND claim_row.subject_id = candidate.subject_id
      AND claim_row.job_id = candidate.job_id
      AND claim_row.claim_id = candidate.claim_id;

    claim_id := candidate.claim_id;
    case_id := candidate.case_id;
    episode_ids := candidate.episode_ids;
    content_hash := candidate.content_hash;
    confidence := candidate.confidence;
    sensitivity := candidate.sensitivity;
    valid_from := candidate.valid_from;
    valid_until := candidate.valid_until;
    observed_at := candidate.observed_at;
    value := candidate.value;
    model_identity := candidate.model_identity;
    prompt_policy_version := candidate.prompt_policy_version;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_consolidation_claim(
    uuid, uuid, uuid, uuid, integer
) FROM PUBLIC;

CREATE FUNCTION memory.complete_consolidation_claim(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid,
    candidate_claim_id uuid,
    candidate_fact_id uuid,
    candidate_revision_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    updated integer;
BEGIN
    PERFORM memory.set_worker_claim_context();

    UPDATE memory.consolidation_claims AS claim_row
    SET lifecycle_state = 'done',
        fact_id = candidate_fact_id,
        revision_id = candidate_revision_id,
        lease_id = NULL,
        lease_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE claim_row.tenant_id = candidate_tenant_id
      AND claim_row.subject_id = candidate_subject_id
      AND claim_row.job_id = candidate_job_id
      AND claim_row.claim_id = candidate_claim_id
      AND claim_row.lifecycle_state = 'leased';

    GET DIAGNOSTICS updated = ROW_COUNT;

    IF updated = 1 THEN
        UPDATE memory.consolidation_jobs AS job_row
        SET claims_done = job_row.claims_done + 1,
            updated_at = clock_timestamp()
        WHERE job_row.tenant_id = candidate_tenant_id
          AND job_row.subject_id = candidate_subject_id
          AND job_row.job_id = candidate_job_id;
    END IF;

    RETURN updated = 1;
END;
$$;

REVOKE ALL ON FUNCTION memory.complete_consolidation_claim(
    uuid, uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC;

CREATE FUNCTION memory.skip_consolidation_claim(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid,
    candidate_claim_id uuid,
    candidate_skip_reason text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    updated integer;
BEGIN
    PERFORM memory.set_worker_claim_context();

    UPDATE memory.consolidation_claims AS claim_row
    SET lifecycle_state = 'skipped',
        skip_reason = candidate_skip_reason,
        lease_id = NULL,
        lease_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE claim_row.tenant_id = candidate_tenant_id
      AND claim_row.subject_id = candidate_subject_id
      AND claim_row.job_id = candidate_job_id
      AND claim_row.claim_id = candidate_claim_id
      AND claim_row.lifecycle_state = 'leased';

    GET DIAGNOSTICS updated = ROW_COUNT;

    IF updated = 1 THEN
        UPDATE memory.consolidation_jobs AS job_row
        SET claims_done = job_row.claims_done + 1,
            updated_at = clock_timestamp()
        WHERE job_row.tenant_id = candidate_tenant_id
          AND job_row.subject_id = candidate_subject_id
          AND job_row.job_id = candidate_job_id;
    END IF;

    RETURN updated = 1;
END;
$$;

REVOKE ALL ON FUNCTION memory.skip_consolidation_claim(
    uuid, uuid, uuid, uuid, text
) FROM PUBLIC;

CREATE FUNCTION memory.release_consolidation_claim(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid,
    candidate_claim_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    updated integer;
BEGIN
    PERFORM memory.set_worker_claim_context();

    UPDATE memory.consolidation_claims AS claim_row
    SET lifecycle_state = 'pending',
        lease_id = NULL,
        lease_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE claim_row.tenant_id = candidate_tenant_id
      AND claim_row.subject_id = candidate_subject_id
      AND claim_row.job_id = candidate_job_id
      AND claim_row.claim_id = candidate_claim_id
      AND claim_row.lifecycle_state = 'leased';

    GET DIAGNOSTICS updated = ROW_COUNT;
    RETURN updated = 1;
END;
$$;

REVOKE ALL ON FUNCTION memory.release_consolidation_claim(
    uuid, uuid, uuid, uuid
) FROM PUBLIC;

CREATE FUNCTION memory.complete_consolidation_job(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.consolidation_jobs%ROWTYPE;
BEGIN
    PERFORM memory.set_worker_claim_context();

    SELECT job.*
    INTO candidate
    FROM memory.consolidation_jobs AS job
    WHERE job.tenant_id = candidate_tenant_id
      AND job.subject_id = candidate_subject_id
      AND job.job_id = candidate_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    IF candidate.claims_done >= candidate.claims_total THEN
        UPDATE memory.consolidation_jobs AS job_row
        SET lifecycle_state = 'complete',
            state_version = job_row.state_version + 1,
            worker_lease_id = NULL,
            worker_lease_expires_at = NULL,
            completed_at = clock_timestamp(),
            updated_at = clock_timestamp()
        WHERE job_row.tenant_id = candidate_tenant_id
          AND job_row.subject_id = candidate_subject_id
          AND job_row.job_id = candidate_job_id;
        RETURN true;
    END IF;

    RETURN false;
END;
$$;

REVOKE ALL ON FUNCTION memory.complete_consolidation_job(
    uuid, uuid, uuid
) FROM PUBLIC;

CREATE FUNCTION memory.fail_consolidation_job(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid,
    candidate_reason text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    updated integer;
BEGIN
    PERFORM memory.set_worker_claim_context();

    UPDATE memory.consolidation_jobs AS job_row
    SET lifecycle_state = 'failed',
        state_version = job_row.state_version + 1,
        worker_lease_id = NULL,
        worker_lease_expires_at = NULL,
        failure_reason = candidate_reason,
        updated_at = clock_timestamp()
    WHERE job_row.tenant_id = candidate_tenant_id
      AND job_row.subject_id = candidate_subject_id
      AND job_row.job_id = candidate_job_id
      AND job_row.lifecycle_state = 'running';

    GET DIAGNOSTICS updated = ROW_COUNT;
    RETURN updated = 1;
END;
$$;

REVOKE ALL ON FUNCTION memory.fail_consolidation_job(
    uuid, uuid, uuid, text
) FROM PUBLIC;

-- Row-level security. The worker claim escape grants the trusted server-side
-- worker; every other session needs the tenant (and where present, subject)
-- scope GUCs, exactly like the other forced-RLS tables.

ALTER TABLE memory.consolidation_interpreter_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_interpreter_configs FORCE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_policies FORCE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.consolidation_claims FORCE ROW LEVEL SECURITY;

CREATE POLICY consolidation_tenant_scope_select
    ON memory.consolidation_interpreter_configs
    FOR SELECT
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY consolidation_tenant_scope_insert
    ON memory.consolidation_interpreter_configs
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_tenant_scope_update
    ON memory.consolidation_interpreter_configs
    FOR UPDATE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_tenant_scope_delete
    ON memory.consolidation_interpreter_configs
    FOR DELETE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_policy_tenant_scope_select
    ON memory.consolidation_policies
    FOR SELECT
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY consolidation_policy_tenant_scope_insert
    ON memory.consolidation_policies
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_policy_tenant_scope_update
    ON memory.consolidation_policies
    FOR UPDATE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_policy_tenant_scope_delete
    ON memory.consolidation_policies
    FOR DELETE
    USING (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY consolidation_jobs_scope_select
    ON memory.consolidation_jobs
    FOR SELECT
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
         AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY consolidation_jobs_scope_insert
    ON memory.consolidation_jobs
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY consolidation_jobs_scope_update
    ON memory.consolidation_jobs
    FOR UPDATE
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
         AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY consolidation_claims_scope_select
    ON memory.consolidation_claims
    FOR SELECT
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
         AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY consolidation_claims_scope_insert
    ON memory.consolidation_claims
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
        AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid
    );

CREATE POLICY consolidation_claims_scope_update
    ON memory.consolidation_claims
    FOR UPDATE
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
         AND subject_id = (NULLIF(current_setting('palimpsest.subject_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

COMMENT ON TABLE memory.consolidation_jobs IS
    'Durable consolidation work items. Rows contain no memory payload.';
COMMENT ON TABLE memory.consolidation_claims IS
    'Claim-level consolidation state with attributable model metadata.';
