CREATE TABLE memory.subject_lifecycles (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    lifecycle_state text NOT NULL DEFAULT 'active' CHECK (
        lifecycle_state IN ('active', 'deletion_pending', 'deleted')
    ),
    state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id)
);

COMMENT ON TABLE memory.subject_lifecycles IS
    'Monotonic subject-wide content fence. Missing rows retain pre-fence active behavior.';

CREATE FUNCTION memory.restrict_subject_lifecycle_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    IF OLD.tenant_id <> NEW.tenant_id OR OLD.subject_id <> NEW.subject_id THEN
        RAISE EXCEPTION 'subject lifecycle scope is immutable'
            USING ERRCODE = '23000';
    END IF;
    IF NOT (
        (OLD.lifecycle_state = 'active' AND NEW.lifecycle_state = 'deletion_pending')
        OR (
            OLD.lifecycle_state = 'deletion_pending'
            AND NEW.lifecycle_state = 'deleted'
        )
    ) THEN
        RAISE EXCEPTION 'subject lifecycle transition is invalid'
            USING ERRCODE = '23000';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'subject lifecycle version must advance by one'
            USING ERRCODE = '23000';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER restrict_subject_lifecycle_mutation
BEFORE UPDATE ON memory.subject_lifecycles
FOR EACH ROW
EXECUTE FUNCTION memory.restrict_subject_lifecycle_mutation();

CREATE FUNCTION memory.transition_subject_to_deletion_pending(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, memory
AS $$
DECLARE
    current_state text;
    current_version bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            candidate_tenant_id::text || ':' || candidate_subject_id::text,
            0
        )
    );

    INSERT INTO memory.subject_lifecycles (
        tenant_id, subject_id, lifecycle_state, state_version
    )
    VALUES (candidate_tenant_id, candidate_subject_id, 'deletion_pending', 1)
    ON CONFLICT (tenant_id, subject_id) DO NOTHING;

    SELECT lifecycle_state, state_version
    INTO current_state, current_version
    FROM memory.subject_lifecycles
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id
    FOR UPDATE;

    IF current_state = 'active' THEN
        UPDATE memory.subject_lifecycles
        SET lifecycle_state = 'deletion_pending',
            state_version = state_version + 1
        WHERE tenant_id = candidate_tenant_id
          AND subject_id = candidate_subject_id
        RETURNING state_version INTO current_version;
    ELSIF current_state <> 'deletion_pending' THEN
        RAISE EXCEPTION 'deleted subject cannot return to deletion_pending'
            USING ERRCODE = '23000';
    END IF;

    RETURN current_version;
END;
$$;

REVOKE ALL ON FUNCTION memory.transition_subject_to_deletion_pending(uuid, uuid)
FROM PUBLIC;

CREATE TABLE memory.subject_content_leases (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    lease_id uuid NOT NULL,
    principal_id text NOT NULL CHECK (char_length(principal_id) BETWEEN 1 AND 255),
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL CHECK (expires_at > acquired_at),
    PRIMARY KEY (tenant_id, subject_id, lease_id),
    FOREIGN KEY (tenant_id, subject_id)
        REFERENCES memory.subject_lifecycles (tenant_id, subject_id)
        ON DELETE RESTRICT
);

COMMENT ON TABLE memory.subject_content_leases IS
    'Bounded content-delivery leases. Rows contain no response or memory payload.';

CREATE FUNCTION memory.reject_subject_content_lease_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    RAISE EXCEPTION 'subject content leases are immutable'
        USING ERRCODE = '23000';
END;
$$;

CREATE TRIGGER reject_subject_content_lease_update
BEFORE UPDATE ON memory.subject_content_leases
FOR EACH ROW
EXECUTE FUNCTION memory.reject_subject_content_lease_update();

INSERT INTO memory.subject_lifecycles (
    tenant_id, subject_id, lifecycle_state, state_version
)
SELECT DISTINCT scope.tenant_id, scope.subject_id, 'active', 0
FROM (
    SELECT tenant_id, subject_id FROM memory.episodes
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.facts
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.checkpoints
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.idempotency_receipts
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.write_audit_receipts
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.outbox_intents
    UNION ALL
    SELECT tenant_id, subject_id FROM memory.retrieval_receipts
) AS scope;

ALTER TABLE memory.subject_lifecycles ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.subject_lifecycles FORCE ROW LEVEL SECURITY;

CREATE POLICY subject_lifecycles_select_scope
ON memory.subject_lifecycles
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE POLICY subject_lifecycles_insert_scope
ON memory.subject_lifecycles
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE POLICY subject_lifecycles_update_scope
ON memory.subject_lifecycles
FOR UPDATE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
)
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE FUNCTION memory.subject_lifecycle_allows_content(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, memory
RETURN NOT EXISTS (
    SELECT 1
    FROM memory.subject_lifecycles AS lifecycle
    WHERE lifecycle.tenant_id = candidate_tenant_id
      AND lifecycle.subject_id = candidate_subject_id
      AND lifecycle.lifecycle_state <> 'active'
);

ALTER TABLE memory.subject_content_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.subject_content_leases FORCE ROW LEVEL SECURITY;

CREATE POLICY subject_content_leases_select_scope
ON memory.subject_content_leases
FOR SELECT
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE POLICY subject_content_leases_insert_scope
ON memory.subject_content_leases
FOR INSERT
WITH CHECK (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    AND memory.subject_lifecycle_allows_content(tenant_id, subject_id)
);

CREATE POLICY subject_content_leases_delete_scope
ON memory.subject_content_leases
FOR DELETE
USING (
    tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
    AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
);

CREATE POLICY episodes_active_subject
ON memory.episodes AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY idempotency_receipts_active_subject
ON memory.idempotency_receipts AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY facts_active_subject
ON memory.facts AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revisions_active_subject
ON memory.fact_revisions AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revision_evidence_active_subject
ON memory.fact_revision_evidence AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY write_audit_receipts_active_subject
ON memory.write_audit_receipts AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY outbox_intents_active_subject
ON memory.outbox_intents AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY checkpoints_active_subject
ON memory.checkpoints AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY checkpoint_revisions_active_subject
ON memory.checkpoint_revisions AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY checkpoint_effect_intents_active_subject
ON memory.checkpoint_effect_intents AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY checkpoint_effect_receipts_active_subject
ON memory.checkpoint_effect_receipts AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revision_governance_active_subject
ON memory.fact_revision_governance AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revision_search_documents_active_subject
ON memory.fact_revision_search_documents AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY retrieval_receipts_active_subject
ON memory.retrieval_receipts AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY retrieval_idempotency_reservations_active_subject
ON memory.retrieval_idempotency_reservations AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY retrieval_manifest_items_active_subject
ON memory.retrieval_manifest_items AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revision_embedding_projections_active_subject
ON memory.fact_revision_embedding_projections AS RESTRICTIVE FOR ALL
USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));
