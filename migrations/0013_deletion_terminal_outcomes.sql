-- Deletion status must distinguish verified live purge from backup disposition.
-- This development deployment has no backup/restore adapter, so completion
-- reports backup as not_configured instead of claiming isolated backup safety.

ALTER TABLE memory.deletion_operations
    ADD COLUMN outcome jsonb;

ALTER TABLE memory.deletion_operations
    ADD CONSTRAINT deletion_operations_outcome_object
    CHECK (outcome IS NULL OR jsonb_typeof(outcome) = 'object');

ALTER TABLE memory.deletion_tombstones
    ADD COLUMN outcome jsonb;

ALTER TABLE memory.deletion_tombstones
    ADD CONSTRAINT deletion_tombstones_outcome_object
    CHECK (outcome IS NULL OR jsonb_typeof(outcome) = 'object');

CREATE FUNCTION memory.set_deletion_operation_outcome()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    IF NEW.lifecycle_state = 'failed' THEN
        NEW.outcome := jsonb_build_object(
            'live_disposition', 'fenced_not_verified',
            'backup_disposition', 'not_configured',
            'backup_policy_id', NULL,
            'deletion_watermark', NULL,
            'earliest_backup_expiry', NULL,
            'restore_gate_version', NULL,
            'verification_digest', NULL
        );
    ELSE
        NEW.outcome := NULL;
    END IF;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION memory.set_deletion_operation_outcome()
FROM PUBLIC;

CREATE TRIGGER deletion_operations_terminal_outcome
BEFORE INSERT OR UPDATE OF lifecycle_state
ON memory.deletion_operations
FOR EACH ROW
EXECUTE FUNCTION memory.set_deletion_operation_outcome();

CREATE FUNCTION memory.set_deletion_tombstone_outcome()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    -- No trusted backup/restore adapter exists in this deployment. Do not
    -- accept transaction-local declarations as evidence of isolation.
    NEW.backup_policy_id := 'not_configured';
    NEW.earliest_backup_expiry := NULL;
    NEW.outcome := jsonb_build_object(
        'live_disposition', 'purged_and_verified',
        'backup_disposition', 'not_configured',
        'backup_policy_id', NULL,
        'deletion_watermark', NULL,
        'earliest_backup_expiry', NULL,
        'restore_gate_version', NULL,
        'verification_digest', rtrim(NEW.verification_digest::text)
    );
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION memory.set_deletion_tombstone_outcome()
FROM PUBLIC;

CREATE TRIGGER deletion_tombstones_terminal_outcome
BEFORE INSERT
ON memory.deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION memory.set_deletion_tombstone_outcome();

DROP FUNCTION memory.poll_deletion_operation(uuid, uuid, uuid);

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
    outcome jsonb,
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
        COALESCE(
            operation.outcome,
            CASE WHEN operation.lifecycle_state = 'failed' THEN jsonb_build_object(
                'live_disposition', 'fenced_not_verified',
                'backup_disposition', 'not_configured',
                'backup_policy_id', NULL,
                'deletion_watermark', NULL,
                'earliest_backup_expiry', NULL,
                'restore_gate_version', NULL,
                'verification_digest', NULL
            ) END
        ),
        operation.updated_at,
        false
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
           COALESCE(
               tombstone.outcome,
               jsonb_build_object(
                   'live_disposition', 'purged_and_verified',
                   'backup_disposition', 'not_configured',
                   'backup_policy_id', NULL,
                   'deletion_watermark', NULL,
                   'earliest_backup_expiry', NULL,
                   'restore_gate_version', NULL,
                   'verification_digest', rtrim(tombstone.verification_digest::text)
               )
           ),
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
