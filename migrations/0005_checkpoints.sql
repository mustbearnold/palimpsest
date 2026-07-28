CREATE TABLE memory.checkpoint_retention_policies (
    retention_policy_id text PRIMARY KEY
        CHECK (btrim(retention_policy_id) <> '' AND length(retention_policy_id) <= 255),
    retention_interval interval NOT NULL
        CHECK (retention_interval >= interval '1 second'),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at))
);

INSERT INTO memory.checkpoint_retention_policies (
    retention_policy_id,
    retention_interval
)
VALUES ('checkpoint-active-30d-v1', interval '30 days');

CREATE TABLE memory.checkpoints (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    checkpoint_id uuid NOT NULL DEFAULT uuidv7()
        CHECK (uuid_extract_version(checkpoint_id) = 7),
    head_revision_id uuid NOT NULL,
    head_revision_number bigint NOT NULL CHECK (head_revision_number > 0),
    retention_policy_id text NOT NULL,
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(updated_at)),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    PRIMARY KEY (tenant_id, subject_id, agent_id, thread_id),
    UNIQUE (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id
    ),
    UNIQUE (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id
    ),
    FOREIGN KEY (retention_policy_id)
        REFERENCES memory.checkpoint_retention_policies (retention_policy_id)
);

CREATE TABLE memory.checkpoint_revisions (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    checkpoint_id uuid NOT NULL,
    revision_id uuid NOT NULL DEFAULT uuidv7()
        CHECK (uuid_extract_version(revision_id) = 7),
    revision_number bigint NOT NULL CHECK (revision_number > 0),
    parent_revision_id uuid,
    state jsonb NOT NULL CHECK (jsonb_typeof(state) IS NOT NULL),
    state_schema_version integer NOT NULL CHECK (state_schema_version > 0),
    state_sha256 character(64) NOT NULL CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    source_type text NOT NULL CHECK (btrim(source_type) <> '' AND length(source_type) <= 255),
    source_uri text,
    external_id text CHECK (external_id IS NULL OR length(external_id) <= 1024),
    sensitivity text NOT NULL CHECK (btrim(sensitivity) <> '' AND length(sensitivity) <= 255),
    retention_policy_id text NOT NULL,
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    writer_principal_id text NOT NULL CHECK (btrim(writer_principal_id) <> ''),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(recorded_at)),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    PRIMARY KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    UNIQUE (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    UNIQUE (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_number
    ),
    CONSTRAINT checkpoint_revisions_checkpoint_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id
    ) REFERENCES memory.checkpoints (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id
    ) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT checkpoint_revisions_parent_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        parent_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    CONSTRAINT checkpoint_revision_root_matches_number CHECK (
        (revision_number = 1) = (parent_revision_id IS NULL)
    ),
    FOREIGN KEY (retention_policy_id)
        REFERENCES memory.checkpoint_retention_policies (retention_policy_id)
);

ALTER TABLE memory.checkpoints
    ADD CONSTRAINT checkpoints_head_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id,
        head_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX checkpoint_revisions_one_root_idx
    ON memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id
    )
    WHERE parent_revision_id IS NULL;

CREATE UNIQUE INDEX checkpoint_revisions_one_successor_idx
    ON memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        parent_revision_id
    )
    WHERE parent_revision_id IS NOT NULL;

CREATE INDEX checkpoint_revisions_scope_recorded_idx
    ON memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_number DESC
    ) INCLUDE (revision_id, parent_revision_id, recorded_at, expires_at);

CREATE FUNCTION memory.prepare_checkpoint_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    predecessor_revision_number bigint;
    predecessor_recorded_at timestamptz;
    current_head_revision_id uuid;
    policy_interval interval;
BEGIN
    SELECT retention_interval
    INTO policy_interval
    FROM memory.checkpoint_retention_policies
    WHERE retention_policy_id = NEW.retention_policy_id
      AND active;

    IF policy_interval IS NULL THEN
        RAISE EXCEPTION 'the checkpoint retention policy is absent or inactive'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'checkpoint_retention_policy_active';
    END IF;

    IF NEW.parent_revision_id IS NULL THEN
        IF NEW.revision_number <> 1 THEN
            RAISE EXCEPTION 'a root checkpoint revision must have revision number 1'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'checkpoint_revision_root_matches_number';
        END IF;

        NEW.recorded_at := clock_timestamp();
    ELSE
        SELECT revision_number, recorded_at
        INTO predecessor_revision_number, predecessor_recorded_at
        FROM memory.checkpoint_revisions
        WHERE tenant_id = NEW.tenant_id
          AND subject_id = NEW.subject_id
          AND agent_id = NEW.agent_id
          AND thread_id = NEW.thread_id
          AND checkpoint_id = NEW.checkpoint_id
          AND revision_id = NEW.parent_revision_id;

        IF predecessor_revision_number IS NULL THEN
            RAISE EXCEPTION 'the parent checkpoint revision does not exist in this lineage'
                USING ERRCODE = '23503',
                      CONSTRAINT = 'checkpoint_revisions_parent_fkey';
        END IF;

        IF NEW.revision_number <> predecessor_revision_number + 1 THEN
            RAISE EXCEPTION 'a checkpoint revision number must immediately follow its parent'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'checkpoint_revision_number_follows_parent';
        END IF;

        SELECT head_revision_id
        INTO current_head_revision_id
        FROM memory.checkpoints
        WHERE tenant_id = NEW.tenant_id
          AND subject_id = NEW.subject_id
          AND agent_id = NEW.agent_id
          AND thread_id = NEW.thread_id
          AND checkpoint_id = NEW.checkpoint_id
        FOR UPDATE;

        IF current_head_revision_id IS DISTINCT FROM NEW.parent_revision_id THEN
            RAISE EXCEPTION 'the checkpoint parent is not the current head'
                USING ERRCODE = '40001',
                      CONSTRAINT = 'checkpoint_revision_parent_is_head';
        END IF;

        NEW.recorded_at := greatest(
            clock_timestamp(),
            predecessor_recorded_at + interval '1 microsecond'
        );
    END IF;

    NEW.expires_at := NEW.recorded_at + policy_interval;

    RETURN NEW;
END;
$$;

CREATE TRIGGER checkpoint_revisions_prepare_insert
BEFORE INSERT ON memory.checkpoint_revisions
FOR EACH ROW EXECUTE FUNCTION memory.prepare_checkpoint_revision();

CREATE FUNCTION memory.prepare_checkpoint_head_transition()
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

CREATE TRIGGER checkpoints_prepare_transition
BEFORE INSERT OR UPDATE OR DELETE ON memory.checkpoints
FOR EACH ROW EXECUTE FUNCTION memory.prepare_checkpoint_head_transition();

CREATE FUNCTION memory.require_checkpoint_revision_is_head()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM memory.checkpoints
        WHERE tenant_id = NEW.tenant_id
          AND subject_id = NEW.subject_id
          AND agent_id = NEW.agent_id
          AND thread_id = NEW.thread_id
          AND checkpoint_id = NEW.checkpoint_id
          AND head_revision_id = NEW.revision_id
          AND head_revision_number = NEW.revision_number
    ) THEN
        RAISE EXCEPTION 'an inserted checkpoint revision must become the scoped head'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'checkpoint_revision_becomes_head';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER checkpoint_revision_becomes_head
AFTER INSERT ON memory.checkpoint_revisions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION memory.require_checkpoint_revision_is_head();

CREATE FUNCTION memory.reject_checkpoint_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER checkpoint_revisions_reject_mutation
BEFORE UPDATE OR DELETE ON memory.checkpoint_revisions
FOR EACH ROW EXECUTE FUNCTION memory.reject_checkpoint_history_mutation();

CREATE TABLE memory.checkpoint_effect_intents (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    checkpoint_id uuid NOT NULL,
    effect_id uuid NOT NULL DEFAULT uuidv7()
        CHECK (uuid_extract_version(effect_id) = 7),
    effect_key text NOT NULL CHECK (length(effect_key) BETWEEN 1 AND 512),
    kind text NOT NULL CHECK (btrim(kind) <> '' AND length(kind) <= 255),
    recovery_mode text NOT NULL CHECK (
        recovery_mode IN ('idempotency_key', 'reconcile')
    ),
    prepared_revision_id uuid NOT NULL,
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(prepared_at)),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    PRIMARY KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        effect_id
    ),
    UNIQUE (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        effect_key
    ),
    CONSTRAINT checkpoint_effect_intents_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        prepared_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    )
);

CREATE FUNCTION memory.prepare_checkpoint_effect_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.prepared_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER checkpoint_effect_intents_prepare_insert
BEFORE INSERT ON memory.checkpoint_effect_intents
FOR EACH ROW EXECUTE FUNCTION memory.prepare_checkpoint_effect_intent();

CREATE TABLE memory.checkpoint_effect_receipts (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    checkpoint_id uuid NOT NULL,
    effect_id uuid NOT NULL,
    completed_revision_id uuid NOT NULL,
    completed_at timestamptz NOT NULL CHECK (isfinite(completed_at)),
    receipt jsonb NOT NULL CHECK (octet_length(receipt::text) <= 65536),
    receipt_sha256 character(64) NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(recorded_at)),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    PRIMARY KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        effect_id
    ),
    CONSTRAINT checkpoint_effect_receipts_intent_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        effect_id
    ) REFERENCES memory.checkpoint_effect_intents (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        effect_id
    ),
    CONSTRAINT checkpoint_effect_receipts_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        completed_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    )
);

CREATE FUNCTION memory.prepare_checkpoint_effect_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    prepared_revision_number bigint;
    completed_revision_number bigint;
BEGIN
    SELECT prepared.revision_number, completed.revision_number
    INTO prepared_revision_number, completed_revision_number
    FROM memory.checkpoint_effect_intents AS effect
    JOIN memory.checkpoint_revisions AS prepared
      ON prepared.tenant_id = effect.tenant_id
     AND prepared.subject_id = effect.subject_id
     AND prepared.agent_id = effect.agent_id
     AND prepared.thread_id = effect.thread_id
     AND prepared.checkpoint_id = effect.checkpoint_id
     AND prepared.revision_id = effect.prepared_revision_id
    JOIN memory.checkpoint_revisions AS completed
      ON completed.tenant_id = NEW.tenant_id
     AND completed.subject_id = NEW.subject_id
     AND completed.agent_id = NEW.agent_id
     AND completed.thread_id = NEW.thread_id
     AND completed.checkpoint_id = NEW.checkpoint_id
     AND completed.revision_id = NEW.completed_revision_id
    WHERE effect.tenant_id = NEW.tenant_id
      AND effect.subject_id = NEW.subject_id
      AND effect.agent_id = NEW.agent_id
      AND effect.thread_id = NEW.thread_id
      AND effect.checkpoint_id = NEW.checkpoint_id
      AND effect.effect_id = NEW.effect_id;

    IF prepared_revision_number IS NULL OR completed_revision_number IS NULL THEN
        RAISE EXCEPTION 'the completed effect or checkpoint revision does not exist in this lineage'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'checkpoint_effect_receipt_lineage';
    END IF;

    IF completed_revision_number <= prepared_revision_number THEN
        RAISE EXCEPTION 'an effect must complete in a later checkpoint revision'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'checkpoint_effect_completion_follows_prepare';
    END IF;

    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER checkpoint_effect_receipts_prepare_insert
BEFORE INSERT ON memory.checkpoint_effect_receipts
FOR EACH ROW EXECUTE FUNCTION memory.prepare_checkpoint_effect_receipt();

CREATE TRIGGER checkpoint_effect_intents_reject_mutation
BEFORE UPDATE OR DELETE ON memory.checkpoint_effect_intents
FOR EACH ROW EXECUTE FUNCTION memory.reject_checkpoint_history_mutation();

CREATE TRIGGER checkpoint_effect_receipts_reject_mutation
BEFORE UPDATE OR DELETE ON memory.checkpoint_effect_receipts
FOR EACH ROW EXECUTE FUNCTION memory.reject_checkpoint_history_mutation();

ALTER TABLE memory.checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.checkpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY checkpoints_select_scope ON memory.checkpoints
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY checkpoints_insert_scope ON memory.checkpoints
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY checkpoints_update_scope ON memory.checkpoints
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.checkpoint_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.checkpoint_revisions FORCE ROW LEVEL SECURITY;
CREATE POLICY checkpoint_revisions_select_scope ON memory.checkpoint_revisions
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY checkpoint_revisions_insert_scope ON memory.checkpoint_revisions
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.checkpoint_effect_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.checkpoint_effect_intents FORCE ROW LEVEL SECURITY;
CREATE POLICY checkpoint_effect_intents_select_scope ON memory.checkpoint_effect_intents
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY checkpoint_effect_intents_insert_scope ON memory.checkpoint_effect_intents
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.checkpoint_effect_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.checkpoint_effect_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY checkpoint_effect_receipts_select_scope ON memory.checkpoint_effect_receipts
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY checkpoint_effect_receipts_insert_scope ON memory.checkpoint_effect_receipts
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.idempotency_receipts
    ADD COLUMN resource_checkpoint_agent_id uuid,
    ADD COLUMN resource_checkpoint_thread_id uuid,
    ADD COLUMN resource_checkpoint_id uuid,
    ADD COLUMN resource_checkpoint_revision_id uuid,
    ADD CONSTRAINT idempotency_receipts_checkpoint_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id
    ) REFERENCES memory.checkpoints (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id
    ),
    ADD CONSTRAINT idempotency_receipts_checkpoint_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id,
        resource_checkpoint_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    DROP CONSTRAINT idempotency_receipts_check,
    ADD CONSTRAINT idempotency_receipts_resource_state_check CHECK (
        (
            state = 'in_progress'
            AND resource_episode_id IS NULL
            AND resource_fact_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND response_status IS NULL
            AND response_body IS NULL
            AND response_etag IS NULL
            AND response_location IS NULL
            AND completed_at IS NULL
        )
        OR
        (
            state = 'completed'
            AND (
                (resource_episode_id IS NOT NULL)::integer
                + (resource_fact_id IS NOT NULL)::integer
                + (resource_checkpoint_id IS NOT NULL)::integer
            ) = 1
            AND (
                resource_checkpoint_id IS NULL
                OR (
                    resource_checkpoint_agent_id IS NOT NULL
                    AND resource_checkpoint_thread_id IS NOT NULL
                    AND resource_checkpoint_revision_id IS NOT NULL
                )
            )
            AND (
                resource_checkpoint_id IS NOT NULL
                OR (
                    resource_checkpoint_agent_id IS NULL
                    AND resource_checkpoint_thread_id IS NULL
                    AND resource_checkpoint_revision_id IS NULL
                )
            )
            AND response_status BETWEEN 200 AND 299
            AND response_body IS NOT NULL
            AND response_etag IS NOT NULL
            AND response_location IS NOT NULL
            AND completed_at IS NOT NULL
        )
    );

CREATE OR REPLACE FUNCTION memory.restrict_idempotency_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
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

ALTER TABLE memory.write_audit_receipts
    ADD COLUMN resource_checkpoint_agent_id uuid,
    ADD COLUMN resource_checkpoint_thread_id uuid,
    ADD COLUMN resource_checkpoint_id uuid,
    ADD COLUMN resource_checkpoint_revision_id uuid,
    ADD CONSTRAINT write_audit_receipts_checkpoint_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id
    ) REFERENCES memory.checkpoints (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id
    ),
    ADD CONSTRAINT write_audit_receipts_checkpoint_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id,
        resource_checkpoint_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    DROP CONSTRAINT write_audit_receipts_check,
    ADD CONSTRAINT write_audit_receipts_resource_check CHECK (
        (
            resource_episode_id IS NOT NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
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
        )
    );

ALTER TABLE memory.outbox_intents
    ADD COLUMN resource_checkpoint_agent_id uuid,
    ADD COLUMN resource_checkpoint_thread_id uuid,
    ADD COLUMN resource_checkpoint_id uuid,
    ADD COLUMN resource_checkpoint_revision_id uuid,
    ADD CONSTRAINT outbox_intents_checkpoint_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id
    ) REFERENCES memory.checkpoints (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id
    ),
    ADD CONSTRAINT outbox_intents_checkpoint_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        resource_checkpoint_agent_id,
        resource_checkpoint_thread_id,
        resource_checkpoint_id,
        resource_checkpoint_revision_id
    ) REFERENCES memory.checkpoint_revisions (
        tenant_id,
        subject_id,
        case_id,
        agent_id,
        thread_id,
        checkpoint_id,
        revision_id
    ),
    DROP CONSTRAINT outbox_intents_check,
    ADD CONSTRAINT outbox_intents_resource_check CHECK (
        (
            resource_episode_id IS NOT NULL
            AND resource_fact_id IS NULL
            AND resource_revision_id IS NULL
            AND resource_checkpoint_id IS NULL
            AND resource_checkpoint_revision_id IS NULL
            AND resource_checkpoint_agent_id IS NULL
            AND resource_checkpoint_thread_id IS NULL
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
        )
    );

CREATE OR REPLACE FUNCTION memory.restrict_outbox_intent_mutation()
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
        OR OLD.resource_checkpoint_agent_id IS DISTINCT FROM NEW.resource_checkpoint_agent_id
        OR OLD.resource_checkpoint_thread_id IS DISTINCT FROM NEW.resource_checkpoint_thread_id
        OR OLD.resource_checkpoint_id IS DISTINCT FROM NEW.resource_checkpoint_id
        OR OLD.resource_checkpoint_revision_id IS DISTINCT FROM NEW.resource_checkpoint_revision_id
        OR OLD.payload <> NEW.payload
        OR OLD.created_at <> NEW.created_at
    THEN
        RAISE EXCEPTION 'invalid outbox intent transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
