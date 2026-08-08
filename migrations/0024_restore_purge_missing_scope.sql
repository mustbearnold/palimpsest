-- spec 016: a scope fenced after a backup has no lifecycle row in the
-- restored copy. The fence ledger is the independent authority: the replay
-- applies the fence by inserting the deleted lifecycle row when the restored
-- copy predates the fence. Previously the replay raised P0002 and blocked the
-- whole restore.

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
        INSERT INTO memory.subject_lifecycles (
            tenant_id, subject_id, lifecycle_state, state_version
        )
        VALUES (candidate_tenant_id, candidate_subject_id, 'deleted', 1)
        ON CONFLICT (tenant_id, subject_id) DO NOTHING;
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

REVOKE ALL ON FUNCTION memory.restore_purge_scope(uuid, uuid) FROM PUBLIC;
