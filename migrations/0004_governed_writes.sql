CREATE TABLE memory.write_audit_receipts (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    audit_id uuid NOT NULL DEFAULT uuidv7(),
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    operation_id text NOT NULL CHECK (btrim(operation_id) <> ''),
    authorization_decision text NOT NULL CHECK (authorization_decision = 'authorized'),
    authorization_context jsonb NOT NULL CHECK (
        jsonb_typeof(authorization_context) = 'object'
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    resource_episode_id uuid,
    resource_fact_id uuid,
    resource_revision_id uuid,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, subject_id, audit_id),
    FOREIGN KEY (tenant_id, subject_id, case_id, resource_episode_id)
        REFERENCES memory.episodes (tenant_id, subject_id, case_id, episode_id),
    FOREIGN KEY (tenant_id, subject_id, resource_fact_id)
        REFERENCES memory.facts (tenant_id, subject_id, fact_id),
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_fact_id,
        resource_revision_id
    ) REFERENCES memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id
    ),
    CHECK (
        (
            resource_episode_id IS NOT NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
        )
        OR
        (
            resource_episode_id IS NULL
            AND resource_fact_id IS NOT NULL
            AND resource_revision_id IS NOT NULL
        )
    )
);

CREATE TABLE memory.outbox_intents (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    intent_id uuid NOT NULL DEFAULT uuidv7(),
    event_type text NOT NULL CHECK (btrim(event_type) <> ''),
    resource_episode_id uuid,
    resource_fact_id uuid,
    resource_revision_id uuid,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    PRIMARY KEY (tenant_id, subject_id, intent_id),
    FOREIGN KEY (tenant_id, subject_id, case_id, resource_episode_id)
        REFERENCES memory.episodes (tenant_id, subject_id, case_id, episode_id),
    FOREIGN KEY (tenant_id, subject_id, resource_fact_id)
        REFERENCES memory.facts (tenant_id, subject_id, fact_id),
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_fact_id,
        resource_revision_id
    ) REFERENCES memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id
    ),
    CHECK (
        (
            resource_episode_id IS NOT NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
        )
        OR
        (
            resource_episode_id IS NULL
            AND resource_fact_id IS NOT NULL
            AND resource_revision_id IS NOT NULL
        )
    )
);

CREATE TRIGGER write_audit_receipts_reject_mutation
BEFORE UPDATE OR DELETE ON memory.write_audit_receipts
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE FUNCTION memory.restrict_outbox_intent_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
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
        OR OLD.payload <> NEW.payload
        OR OLD.created_at <> NEW.created_at
    THEN
        RAISE EXCEPTION 'invalid outbox intent transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_intents_restrict_mutation
BEFORE UPDATE OR DELETE ON memory.outbox_intents
FOR EACH ROW EXECUTE FUNCTION memory.restrict_outbox_intent_mutation();

ALTER TABLE memory.write_audit_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.write_audit_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY write_audit_receipts_select_scope ON memory.write_audit_receipts
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY write_audit_receipts_insert_scope ON memory.write_audit_receipts
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.outbox_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.outbox_intents FORCE ROW LEVEL SECURITY;
CREATE POLICY outbox_intents_select_scope ON memory.outbox_intents
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY outbox_intents_insert_scope ON memory.outbox_intents
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY outbox_intents_publish_scope ON memory.outbox_intents
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND published_at IS NULL
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND published_at IS NOT NULL
    );
