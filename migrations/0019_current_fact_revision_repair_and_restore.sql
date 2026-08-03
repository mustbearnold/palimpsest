ALTER POLICY fact_revision_current_active_subject
    ON memory.fact_revision_current
USING (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
)
WITH CHECK (
    memory.subject_lifecycle_allows_content(tenant_id, subject_id)
    OR memory.deletion_workflow_allows(tenant_id, subject_id)
);

CREATE POLICY fact_revision_current_repair_delete
    ON memory.fact_revision_current
    FOR DELETE
    USING (
        current_user = pg_get_userbyid((
            SELECT relowner
            FROM pg_catalog.pg_class
            WHERE oid = 'memory.fact_revision_current'::pg_catalog.regclass
        ))
        AND current_setting('palimpsest.fact_revision_current_repair', true)
            = 'palimpsest-fact-current-repair-v1'
        AND tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE FUNCTION memory.rebuild_fact_revision_current(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    rebuilt bigint;
    table_owner text;
BEGIN
    SELECT pg_get_userbyid(relowner)
    INTO table_owner
    FROM pg_catalog.pg_class
    WHERE oid = 'memory.fact_revision_current'::pg_catalog.regclass;

    IF candidate_tenant_id IS NULL
       OR candidate_subject_id IS NULL
       OR session_user <> table_owner THEN
        RAISE EXCEPTION 'current fact-revision repair is not authorized'
            USING ERRCODE = '42501';
    END IF;

    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    PERFORM set_config(
        'palimpsest.fact_revision_current_repair',
        'palimpsest-fact-current-repair-v1',
        true
    );

    DELETE FROM memory.fact_revision_current
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;

    INSERT INTO memory.fact_revision_current (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        revision_no,
        observed_at,
        recorded_at,
        valid_during,
        namespace,
        fact_key,
        value,
        confidence,
        sensitivity,
        content_sha256,
        schema_version
    )
    SELECT DISTINCT ON (revision.tenant_id, revision.subject_id, revision.fact_id)
        revision.tenant_id,
        revision.subject_id,
        revision.case_id,
        revision.fact_id,
        revision.revision_id,
        revision.revision_no,
        revision.observed_at,
        revision.recorded_at,
        revision.valid_during,
        fact.namespace,
        fact.fact_key,
        revision.value,
        revision.confidence,
        revision.sensitivity,
        revision.content_sha256,
        revision.schema_version
    FROM memory.fact_revisions AS revision
    JOIN memory.facts AS fact
      ON fact.tenant_id = revision.tenant_id
     AND fact.subject_id = revision.subject_id
     AND fact.case_id = revision.case_id
     AND fact.fact_id = revision.fact_id
    WHERE revision.tenant_id = candidate_tenant_id
      AND revision.subject_id = candidate_subject_id
    ORDER BY revision.tenant_id, revision.subject_id, revision.fact_id,
        revision.revision_no DESC, revision.revision_id;

    GET DIAGNOSTICS rebuilt = ROW_COUNT;
    RETURN rebuilt;
END;
$$;

REVOKE ALL ON FUNCTION memory.rebuild_fact_revision_current(uuid, uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION memory.restore_purge_scope(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    residual bigint;
BEGIN
    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    IF NOT memory.restore_replay_allows(candidate_tenant_id, candidate_subject_id) THEN
        RAISE EXCEPTION 'restore replay scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    DELETE FROM memory.retrieval_manifest_items
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.retrieval_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.retrieval_idempotency_reservations
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.export_manifest_items
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.export_operations
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_evidence
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_governance
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_search_documents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_embedding_projections
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.outbox_intents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.idempotency_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.write_audit_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_effect_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_effect_intents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_revisions
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoints
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_current
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revisions
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.facts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.episodes
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.subject_content_leases
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;

    UPDATE memory.subject_lifecycles
    SET lifecycle_state = 'deleted',
        state_version = state_version + 1
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id
      AND lifecycle_state <> 'deleted';
    IF NOT FOUND THEN
        IF NOT EXISTS (
            SELECT 1
            FROM memory.subject_lifecycles
            WHERE tenant_id = candidate_tenant_id
              AND subject_id = candidate_subject_id
              AND lifecycle_state = 'deleted'
        ) THEN
            RAISE EXCEPTION 'restore replay subject lifecycle is missing'
                USING ERRCODE = 'P0002';
        END IF;
    END IF;

    SELECT
        (SELECT count(*) FROM memory.episodes
         WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.facts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revisions
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_current
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
        + (SELECT count(*) FROM memory.export_manifest_items
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.export_operations
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.subject_content_leases
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
    INTO residual;
    RETURN residual;
END;
$$;
