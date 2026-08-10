-- 0028_wiki_lint.sql
-- Wiki lint worker (spec 017 P4, AC8).
--
-- The lint pass is an operation, not state (R9). A periodic worker job
-- checks contradictions, orphans, stale claims, and provenance gaps. The
-- job lifecycle follows the spec 011 worker pattern: jobs and claims,
-- bounded leases, crash-resumable (an expired lease makes a running job
-- claimable again). The worker writes lint state to the governed fact
-- namespace `wiki/lint` and generates a new open question in the
-- `open-questions` namespace through the governed fact path (001 R9).

CREATE TABLE memory.wiki_lint_jobs (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    job_id uuid NOT NULL,
    lifecycle_state text NOT NULL DEFAULT 'pending'
        CHECK (
            lifecycle_state IN ('pending', 'running', 'complete', 'failed')
        ),
    state_version bigint NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    worker_lease_id uuid,
    worker_lease_expires_at timestamptz,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    retry_at timestamptz,
    failure_reason text
        CHECK (
            failure_reason IS NULL
            OR (
                length(failure_reason) BETWEEN 1 AND 200
                AND failure_reason ~ '^[a-z0-9 ._-]+$'
            )
        ),
    contradictions integer NOT NULL DEFAULT 0 CHECK (contradictions >= 0),
    orphans integer NOT NULL DEFAULT 0 CHECK (orphans >= 0),
    stale_claims integer NOT NULL DEFAULT 0 CHECK (stale_claims >= 0),
    provenance_gaps integer NOT NULL DEFAULT 0 CHECK (provenance_gaps >= 0),
    lint_fact_id uuid,
    question_fact_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    principal_id text NOT NULL
        CHECK (char_length(principal_id) BETWEEN 1 AND 255),
    idempotency_key_digest character(64) NOT NULL
        CHECK (idempotency_key_digest ~ '^[0-9a-f]{64}$'),
    request_fingerprint character(64) NOT NULL
        CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, subject_id, job_id)
);

CREATE INDEX wiki_lint_jobs_claimable_idx
    ON memory.wiki_lint_jobs (created_at)
    WHERE lifecycle_state IN ('pending', 'running');

CREATE UNIQUE INDEX wiki_lint_jobs_idempotency_key_idx
    ON memory.wiki_lint_jobs
    (tenant_id, subject_id, principal_id, idempotency_key_digest);

-- Claim the oldest claimable wiki lint job for a worker, or nothing.
-- A running job whose lease has expired is claimable again: the crash of a
-- worker mid-scan cannot strand a job.
CREATE FUNCTION memory.claim_next_wiki_lint_job(
    candidate_worker_id uuid,
    candidate_lease_seconds integer
)
RETURNS TABLE(
    tenant_id uuid,
    subject_id uuid,
    job_id uuid,
    lifecycle_state text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    candidate memory.wiki_lint_jobs%ROWTYPE;
BEGIN
    PERFORM memory.set_worker_claim_context();

    SELECT job.*
    INTO candidate
    FROM memory.wiki_lint_jobs AS job
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
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE memory.wiki_lint_jobs AS job_row
    SET lifecycle_state = 'running',
        state_version = job_row.state_version + 1,
        worker_lease_id = candidate_worker_id,
        worker_lease_expires_at =
            clock_timestamp() + make_interval(secs => candidate_lease_seconds),
        updated_at = clock_timestamp()
    WHERE job_row.tenant_id = candidate.tenant_id
      AND job_row.subject_id = candidate.subject_id
      AND job_row.job_id = candidate.job_id
      AND job_row.lifecycle_state = 'pending';

    IF NOT FOUND THEN
        RETURN;
    END IF;

    tenant_id := candidate.tenant_id;
    subject_id := candidate.subject_id;
    job_id := candidate.job_id;
    lifecycle_state := 'running';
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.claim_next_wiki_lint_job(uuid, integer)
FROM PUBLIC;

ALTER TABLE memory.wiki_lint_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.wiki_lint_jobs FORCE ROW LEVEL SECURITY;

CREATE POLICY wiki_lint_jobs_scope_select
    ON memory.wiki_lint_jobs
    FOR SELECT
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );

CREATE POLICY wiki_lint_jobs_scope_insert
    ON memory.wiki_lint_jobs
    FOR INSERT
    WITH CHECK (
        tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid
    );

CREATE POLICY wiki_lint_jobs_scope_update
    ON memory.wiki_lint_jobs
    FOR UPDATE
    USING (
        (tenant_id = (NULLIF(current_setting('palimpsest.tenant_id', true), ''))::uuid)
        OR current_setting('palimpsest.worker_claim', true) = 'palimpsest-worker-v1'
    );
