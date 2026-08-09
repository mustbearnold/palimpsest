-- The worker-claim existence check moves into a SECURITY DEFINER function
-- so the worker-context string lives in one place
-- (set_worker_claim_context) instead of being re-created in Rust.

CREATE FUNCTION memory.consolidation_job_has_in_flight_claims(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_job_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    PERFORM memory.set_worker_claim_context();

    RETURN EXISTS(
        SELECT 1
        FROM memory.consolidation_claims
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
          AND job_id = candidate_job_id
          AND lifecycle_state = 'leased'
          AND lease_expires_at > clock_timestamp()
    );
END;
$$;

REVOKE ALL ON FUNCTION memory.consolidation_job_has_in_flight_claims(
    uuid, uuid, uuid
) FROM PUBLIC;
