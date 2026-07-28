ALTER TABLE memory.episodes
    ADD CONSTRAINT episodes_tenant_subject_case_episode_key
    UNIQUE (tenant_id, subject_id, case_id, episode_id);

CREATE TABLE memory.facts (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    namespace text NOT NULL CHECK (btrim(namespace) <> ''),
    fact_key text NOT NULL CHECK (btrim(fact_key) <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, case_id, fact_id),
    UNIQUE (tenant_id, subject_id, fact_id),
    UNIQUE (tenant_id, subject_id, case_id, namespace, fact_key)
);

CREATE TABLE memory.fact_revisions (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    revision_no bigint NOT NULL CHECK (revision_no > 0),
    supersedes_revision_id uuid,
    observed_at timestamptz NOT NULL CHECK (isfinite(observed_at)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(recorded_at)),
    valid_during tstzrange NOT NULL,
    value jsonb NOT NULL CHECK (value <> 'null'::jsonb),
    confidence numeric(5, 4) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    writer_principal_id text NOT NULL CHECK (btrim(writer_principal_id) <> ''),
    write_policy_id text NOT NULL CHECK (btrim(write_policy_id) <> ''),
    write_policy_version text NOT NULL CHECK (btrim(write_policy_version) <> ''),
    sensitivity text NOT NULL CHECK (btrim(sensitivity) <> ''),
    retention_policy_id text NOT NULL CHECK (btrim(retention_policy_id) <> ''),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    content_sha256 character(64) NOT NULL
        CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, subject_id, case_id, fact_id, revision_id),
    UNIQUE (tenant_id, subject_id, case_id, fact_id, revision_no),
    CONSTRAINT fact_revisions_fact_fkey
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id)
        REFERENCES memory.facts (tenant_id, subject_id, case_id, fact_id),
    CONSTRAINT fact_revisions_supersedes_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        supersedes_revision_id
    ) REFERENCES memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id
    ),
    CONSTRAINT fact_revision_root_number_matches_predecessor CHECK (
        (revision_no = 1) = (supersedes_revision_id IS NULL)
    ),
    CONSTRAINT fact_revision_valid_during_is_half_open CHECK (
        NOT isempty(valid_during)
        AND NOT lower_inf(valid_during)
        AND lower_inc(valid_during)
        AND NOT upper_inc(valid_during)
        AND isfinite(lower(valid_during))
        AND (upper_inf(valid_during) OR isfinite(upper(valid_during)))
    )
);

CREATE UNIQUE INDEX fact_revisions_one_root_idx
    ON memory.fact_revisions (tenant_id, subject_id, case_id, fact_id)
    WHERE supersedes_revision_id IS NULL;

CREATE UNIQUE INDEX fact_revisions_one_successor_idx
    ON memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        supersedes_revision_id
    )
    WHERE supersedes_revision_id IS NOT NULL;

CREATE INDEX fact_revisions_temporal_lookup_idx
    ON memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_no DESC
    )
    INCLUDE (recorded_at, valid_during, revision_id, supersedes_revision_id);

CREATE TABLE memory.fact_revision_evidence (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    episode_id uuid NOT NULL,
    evidence_role text NOT NULL CHECK (btrim(evidence_role) <> ''),
    PRIMARY KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        episode_id
    ),
    CONSTRAINT fact_revision_evidence_revision_fkey
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id
    ) REFERENCES memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id
    ),
    CONSTRAINT fact_revision_evidence_episode_fkey
    FOREIGN KEY (tenant_id, subject_id, case_id, episode_id)
        REFERENCES memory.episodes (tenant_id, subject_id, case_id, episode_id)
);

CREATE INDEX fact_revision_evidence_episode_lookup_idx
    ON memory.fact_revision_evidence (
        tenant_id,
        subject_id,
        case_id,
        episode_id
    );

CREATE FUNCTION memory.prepare_fact_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    predecessor_revision_no bigint;
    predecessor_recorded_at timestamptz;
BEGIN
    IF NEW.supersedes_revision_id IS NULL THEN
        IF NEW.revision_no <> 1 THEN
            RAISE EXCEPTION 'a root fact revision must have revision number 1'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'fact_revision_root_number_matches_predecessor';
        END IF;

        NEW.recorded_at := clock_timestamp();
        RETURN NEW;
    END IF;

    SELECT revision_no, recorded_at
    INTO predecessor_revision_no, predecessor_recorded_at
    FROM memory.fact_revisions
    WHERE tenant_id = NEW.tenant_id
      AND subject_id = NEW.subject_id
      AND case_id = NEW.case_id
      AND fact_id = NEW.fact_id
      AND revision_id = NEW.supersedes_revision_id;

    IF predecessor_revision_no IS NULL THEN
        RAISE EXCEPTION 'the superseded fact revision does not exist in this scope'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'fact_revisions_supersedes_revision_fkey';
    END IF;

    IF NEW.revision_no <> predecessor_revision_no + 1 THEN
        RAISE EXCEPTION 'a fact revision number must immediately follow its predecessor'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'fact_revision_number_follows_predecessor';
    END IF;

    NEW.recorded_at := greatest(
        clock_timestamp(),
        predecessor_recorded_at + interval '1 microsecond'
    );

    RETURN NEW;
END;
$$;

CREATE TRIGGER fact_revisions_prepare_insert
BEFORE INSERT ON memory.fact_revisions
FOR EACH ROW EXECUTE FUNCTION memory.prepare_fact_revision();

CREATE FUNCTION memory.require_fact_revision_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM memory.fact_revision_evidence
        WHERE tenant_id = NEW.tenant_id
          AND subject_id = NEW.subject_id
          AND case_id = NEW.case_id
          AND fact_id = NEW.fact_id
          AND revision_id = NEW.revision_id
    ) THEN
        RAISE EXCEPTION 'every fact revision requires attributable episode evidence'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'fact_revision_requires_evidence';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER fact_revision_requires_evidence
AFTER INSERT ON memory.fact_revisions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION memory.require_fact_revision_evidence();

CREATE FUNCTION memory.reject_fact_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER facts_reject_mutation
BEFORE UPDATE OR DELETE ON memory.facts
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER fact_revisions_reject_mutation
BEFORE UPDATE OR DELETE ON memory.fact_revisions
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER fact_revision_evidence_reject_mutation
BEFORE UPDATE OR DELETE ON memory.fact_revision_evidence
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

ALTER TABLE memory.facts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.facts FORCE ROW LEVEL SECURITY;

CREATE POLICY facts_select_scope ON memory.facts
    FOR SELECT
    USING (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );

CREATE POLICY facts_insert_scope ON memory.facts
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );

ALTER TABLE memory.fact_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revisions FORCE ROW LEVEL SECURITY;

CREATE POLICY fact_revisions_select_scope ON memory.fact_revisions
    FOR SELECT
    USING (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );

CREATE POLICY fact_revisions_insert_scope ON memory.fact_revisions
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );

ALTER TABLE memory.fact_revision_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_evidence FORCE ROW LEVEL SECURITY;

CREATE POLICY fact_revision_evidence_select_scope
    ON memory.fact_revision_evidence
    FOR SELECT
    USING (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );

CREATE POLICY fact_revision_evidence_insert_scope
    ON memory.fact_revision_evidence
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    );
