-- Let a worker release its operation claim after recording an external target
-- failure. The subject remains fenced and the target remains retryable, while
-- another worker can resume without waiting for the full claim lease.

CREATE FUNCTION memory.release_deletion_operation_lease(
    candidate_tenant_id uuid,
    candidate_subject_id uuid,
    candidate_operation_id uuid,
    candidate_worker_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    IF candidate_tenant_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
       OR candidate_subject_id IS DISTINCT FROM
            NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid THEN
        RAISE EXCEPTION 'deletion operation lease scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    UPDATE memory.deletion_operations AS operation
    SET worker_lease_id = NULL,
        worker_lease_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE operation.tenant_id = candidate_tenant_id
      AND operation.subject_id = candidate_subject_id
      AND operation.operation_id = candidate_operation_id
      AND operation.lifecycle_state IN ('draining', 'fenced', 'purging', 'retry_wait')
      AND operation.worker_lease_id = candidate_worker_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'deletion operation worker lease is not held'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION memory.release_deletion_operation_lease(
    uuid, uuid, uuid, uuid
)
FROM PUBLIC;
