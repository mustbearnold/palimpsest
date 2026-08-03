CREATE TABLE memory.fact_revision_current (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    revision_no bigint NOT NULL CHECK (revision_no > 0),
    observed_at timestamptz NOT NULL CHECK (isfinite(observed_at)),
    recorded_at timestamptz NOT NULL CHECK (isfinite(recorded_at)),
    valid_during tstzrange NOT NULL CHECK (
        NOT isempty(valid_during)
        AND NOT lower_inf(valid_during)
        AND lower_inc(valid_during)
        AND NOT upper_inc(valid_during)
        AND isfinite(lower(valid_during))
        AND (upper_inf(valid_during) OR isfinite(upper(valid_during)))
    ),
    namespace text NOT NULL CHECK (btrim(namespace) <> '' AND length(namespace) <= 255),
    fact_key text NOT NULL CHECK (btrim(fact_key) <> '' AND length(fact_key) <= 512),
    value jsonb NOT NULL CHECK (value <> 'null'::jsonb),
    confidence numeric(5, 4) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    sensitivity text NOT NULL CHECK (btrim(sensitivity) <> '' AND length(sensitivity) <= 255),
    content_sha256 character(64) NOT NULL CHECK (
        content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, fact_id),
    CONSTRAINT fact_revision_current_revision_fkey
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id, revision_id)
        REFERENCES memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ) ON DELETE CASCADE
);

CREATE INDEX fact_revision_current_case_lookup_idx
    ON memory.fact_revision_current (
        tenant_id, subject_id, case_id, fact_id
    );

CREATE FUNCTION memory.populate_fact_revision_current()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory
AS $$
BEGIN
    INSERT INTO memory.fact_revision_current AS existing (
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
    SELECT
        NEW.tenant_id,
        NEW.subject_id,
        NEW.case_id,
        NEW.fact_id,
        NEW.revision_id,
        NEW.revision_no,
        NEW.observed_at,
        NEW.recorded_at,
        NEW.valid_during,
        fact.namespace,
        fact.fact_key,
        NEW.value,
        NEW.confidence,
        NEW.sensitivity,
        NEW.content_sha256,
        NEW.schema_version
    FROM memory.facts AS fact
    WHERE fact.tenant_id = NEW.tenant_id
      AND fact.subject_id = NEW.subject_id
      AND fact.case_id = NEW.case_id
      AND fact.fact_id = NEW.fact_id
    ON CONFLICT (tenant_id, subject_id, fact_id) DO UPDATE
    SET case_id = EXCLUDED.case_id,
        revision_id = EXCLUDED.revision_id,
        revision_no = EXCLUDED.revision_no,
        observed_at = EXCLUDED.observed_at,
        recorded_at = EXCLUDED.recorded_at,
        valid_during = EXCLUDED.valid_during,
        namespace = EXCLUDED.namespace,
        fact_key = EXCLUDED.fact_key,
        value = EXCLUDED.value,
        confidence = EXCLUDED.confidence,
        sensitivity = EXCLUDED.sensitivity,
        content_sha256 = EXCLUDED.content_sha256,
        schema_version = EXCLUDED.schema_version
    WHERE existing.revision_no < EXCLUDED.revision_no
       OR (
           existing.revision_no = EXCLUDED.revision_no
           AND existing.revision_id < EXCLUDED.revision_id
       );

    RETURN NULL;
END;
$$;

CREATE TRIGGER fact_revisions_populate_current
AFTER INSERT ON memory.fact_revisions
FOR EACH ROW EXECUTE FUNCTION memory.populate_fact_revision_current();

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
ORDER BY revision.tenant_id, revision.subject_id, revision.fact_id,
    revision.revision_no DESC, revision.revision_id;

ALTER TABLE memory.fact_revision_current ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_current FORCE ROW LEVEL SECURITY;

CREATE POLICY fact_revision_current_select_scope
    ON memory.fact_revision_current
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY fact_revision_current_insert_scope
    ON memory.fact_revision_current
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY fact_revision_current_update_scope
    ON memory.fact_revision_current
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
