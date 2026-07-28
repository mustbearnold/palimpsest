CREATE TABLE memory.fact_retention_policies (
    retention_policy_id text PRIMARY KEY CHECK (
        btrim(retention_policy_id) <> '' AND length(retention_policy_id) <= 255
    ),
    retention_interval interval CHECK (
        retention_interval IS NULL OR retention_interval > interval '0 seconds'
    ),
    policy_origin text NOT NULL CHECK (
        policy_origin IN ('builtin', 'legacy_backfill', 'migration')
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at))
);

INSERT INTO memory.fact_retention_policies (
    retention_policy_id, retention_interval, policy_origin, schema_version
)
VALUES ('standard', NULL, 'builtin', 1);

-- Before this migration, fact retention identifiers were accepted without a
-- policy registry or expiry. Preserve that historical behavior explicitly
-- instead of inventing a finite expiry during backfill.
INSERT INTO memory.fact_retention_policies (
    retention_policy_id, retention_interval, policy_origin, schema_version
)
SELECT DISTINCT retention_policy_id, NULL::interval, 'legacy_backfill', 1
FROM memory.fact_revisions
ON CONFLICT (retention_policy_id) DO NOTHING;

CREATE TABLE memory.search_projection_schemas (
    projection_schema_version integer PRIMARY KEY CHECK (
        projection_schema_version > 0
    ),
    projection_document jsonb NOT NULL CHECK (
        jsonb_typeof(projection_document) = 'object'
    ),
    projection_sha256 character(64) NOT NULL UNIQUE CHECK (
        projection_sha256 ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    UNIQUE (projection_schema_version, projection_sha256)
);

WITH projection(projection_schema_version, projection_document) AS (
    VALUES (
        1,
        jsonb_build_object(
            'configuration', 'pg_catalog.simple',
            'namespace_weight', 'A',
            'key_weight', 'A',
            'value_weight', 'B',
            'serialization', 'jsonb_text_v1'
        )
    )
)
INSERT INTO memory.search_projection_schemas (
    projection_schema_version, projection_document, projection_sha256
)
SELECT
    projection_schema_version,
    projection_document,
    encode(
        sha256(convert_to(projection_document::text, 'UTF8')),
        'hex'
    )
FROM projection;

CREATE TABLE memory.lexical_retrieval_policies (
    policy_id text NOT NULL CHECK (
        btrim(policy_id) <> '' AND length(policy_id) <= 255
    ),
    policy_version text NOT NULL CHECK (
        btrim(policy_version) <> '' AND length(policy_version) <= 255
    ),
    policy_document jsonb NOT NULL CHECK (
        jsonb_typeof(policy_document) = 'object'
    ),
    policy_sha256 character(64) NOT NULL CHECK (
        policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    PRIMARY KEY (policy_id, policy_version),
    UNIQUE (policy_id, policy_version, policy_sha256)
);

WITH policy(policy_id, policy_version, policy_document) AS (
    VALUES (
        'retrieval-lexical-v1',
        '1',
        jsonb_build_object(
            'candidate_limit', 50,
            'default_page_size', 10,
            'exact_identity_precedence', true,
            'fts_configuration', 'pg_catalog.simple',
            'fts_rank', 'ts_rank_cd',
            'fts_rank_normalization', 32,
            'maximum_page_size', 50,
            'score_scale', 12,
            'tie_break', jsonb_build_array(
                'exact_identity_rank_asc_nulls_last',
                'lexical_rank_asc_nulls_last',
                'fact_id_asc',
                'revision_id_asc'
            )
        )
    )
)
INSERT INTO memory.lexical_retrieval_policies (
    policy_id, policy_version, policy_document, policy_sha256, schema_version
)
SELECT
    policy_id,
    policy_version,
    policy_document,
    encode(sha256(convert_to(policy_document::text, 'UTF8')), 'hex'),
    1
FROM policy;

CREATE TABLE memory.fact_revision_governance (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    retention_policy_id text NOT NULL,
    retention_expires_at timestamptz CHECK (
        retention_expires_at IS NULL OR isfinite(retention_expires_at)
    ),
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('active', 'deletion_pending', 'deleted')
    ),
    lifecycle_changed_at timestamptz NOT NULL CHECK (
        isfinite(lifecycle_changed_at)
    ),
    recency_profile_id text NOT NULL CHECK (
        btrim(recency_profile_id) <> '' AND length(recency_profile_id) <= 255
    ),
    importance numeric(5, 4) NOT NULL CHECK (importance BETWEEN 0 AND 1),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, case_id, fact_id, revision_id),
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id, revision_id)
        REFERENCES memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ),
    FOREIGN KEY (retention_policy_id)
        REFERENCES memory.fact_retention_policies (retention_policy_id)
);

CREATE INDEX fact_revision_governance_visibility_idx
    ON memory.fact_revision_governance (
        tenant_id,
        subject_id,
        lifecycle_state,
        retention_expires_at,
        fact_id,
        revision_id
    );

CREATE FUNCTION memory.fact_search_vector_v1(
    fact_namespace text,
    fact_key text,
    fact_value jsonb
)
RETURNS tsvector
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT
        setweight(to_tsvector('pg_catalog.simple'::regconfig, fact_namespace), 'A')
        || setweight(to_tsvector('pg_catalog.simple'::regconfig, fact_key), 'A')
        || setweight(
            to_tsvector('pg_catalog.simple'::regconfig, fact_value::text),
            'B'
        )
$$;

CREATE FUNCTION memory.fact_projection_sha256_v1(
    fact_namespace text,
    fact_key text,
    fact_value jsonb
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT encode(
        sha256(
            convert_to(
                '1' || chr(31) || fact_namespace || chr(31)
                || fact_key || chr(31) || fact_value::text,
                'UTF8'
            )
        ),
        'hex'
    )
$$;

CREATE TABLE memory.fact_revision_search_documents (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    projection_schema_version integer NOT NULL,
    projection_schema_sha256 character(64) NOT NULL,
    source_content_sha256 character(64) NOT NULL CHECK (
        source_content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    projection_sha256 character(64) NOT NULL CHECK (
        projection_sha256 ~ '^[0-9a-f]{64}$'
    ),
    search_vector tsvector NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(generated_at)),
    PRIMARY KEY (tenant_id, subject_id, case_id, fact_id, revision_id),
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id, revision_id)
        REFERENCES memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ),
    FOREIGN KEY (projection_schema_version, projection_schema_sha256)
        REFERENCES memory.search_projection_schemas (
            projection_schema_version, projection_sha256
        )
);

CREATE INDEX fact_revision_search_documents_gin_idx
    ON memory.fact_revision_search_documents
    USING gin (search_vector);

CREATE INDEX fact_revision_search_documents_scope_idx
    ON memory.fact_revision_search_documents (
        tenant_id, subject_id, fact_id, revision_id
    );

CREATE FUNCTION memory.populate_fact_revision_retrieval_metadata()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    retention_duration interval;
    stored_namespace text;
    stored_key text;
    projection_digest character(64);
BEGIN
    SELECT retention_interval
    INTO retention_duration
    FROM memory.fact_retention_policies
    WHERE retention_policy_id = NEW.retention_policy_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'fact retention policy is not registered'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'fact_revision_governance_retention_policy_known';
    END IF;

    SELECT namespace, fact_key
    INTO stored_namespace, stored_key
    FROM memory.facts
    WHERE tenant_id = NEW.tenant_id
      AND subject_id = NEW.subject_id
      AND case_id = NEW.case_id
      AND fact_id = NEW.fact_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'fact metadata is missing for retrieval projection'
            USING ERRCODE = '23503',
                  CONSTRAINT = 'fact_revision_search_document_fact_known';
    END IF;

    SELECT projection_sha256
    INTO projection_digest
    FROM memory.search_projection_schemas
    WHERE projection_schema_version = 1;

    INSERT INTO memory.fact_revision_governance (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        retention_policy_id,
        retention_expires_at,
        lifecycle_state,
        lifecycle_changed_at,
        recency_profile_id,
        importance,
        schema_version
    )
    VALUES (
        NEW.tenant_id,
        NEW.subject_id,
        NEW.case_id,
        NEW.fact_id,
        NEW.revision_id,
        NEW.retention_policy_id,
        CASE
            WHEN retention_duration IS NULL THEN NULL
            ELSE NEW.recorded_at + retention_duration
        END,
        'active',
        NEW.recorded_at,
        'stable-v1',
        0.5000,
        1
    );

    INSERT INTO memory.fact_revision_search_documents (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        projection_schema_version,
        projection_schema_sha256,
        source_content_sha256,
        projection_sha256,
        search_vector
    )
    VALUES (
        NEW.tenant_id,
        NEW.subject_id,
        NEW.case_id,
        NEW.fact_id,
        NEW.revision_id,
        1,
        projection_digest,
        NEW.content_sha256,
        memory.fact_projection_sha256_v1(
            stored_namespace,
            stored_key,
            NEW.value
        ),
        memory.fact_search_vector_v1(
            stored_namespace,
            stored_key,
            NEW.value
        )
    );

    RETURN NULL;
END;
$$;

CREATE TRIGGER fact_revisions_populate_retrieval_metadata
AFTER INSERT ON memory.fact_revisions
FOR EACH ROW EXECUTE FUNCTION memory.populate_fact_revision_retrieval_metadata();

-- Install the trigger before backfilling so writes from an older live server
-- cannot land in the gap between the snapshot and trigger creation. The
-- backfills are idempotent because a concurrent post-trigger write may already
-- have populated either companion row.
INSERT INTO memory.fact_retention_policies (
    retention_policy_id, retention_interval, policy_origin, schema_version
)
SELECT DISTINCT retention_policy_id, NULL::interval, 'legacy_backfill', 1
FROM memory.fact_revisions
ON CONFLICT (retention_policy_id) DO NOTHING;

INSERT INTO memory.fact_revision_governance (
    tenant_id,
    subject_id,
    case_id,
    fact_id,
    revision_id,
    retention_policy_id,
    retention_expires_at,
    lifecycle_state,
    lifecycle_changed_at,
    recency_profile_id,
    importance,
    schema_version
)
SELECT
    revision.tenant_id,
    revision.subject_id,
    revision.case_id,
    revision.fact_id,
    revision.revision_id,
    revision.retention_policy_id,
    CASE
        WHEN policy.retention_interval IS NULL THEN NULL
        ELSE revision.recorded_at + policy.retention_interval
    END,
    'active',
    revision.recorded_at,
    'stable-v1',
    0.5000,
    1
FROM memory.fact_revisions AS revision
JOIN memory.fact_retention_policies AS policy
  ON policy.retention_policy_id = revision.retention_policy_id
ON CONFLICT DO NOTHING;

INSERT INTO memory.fact_revision_search_documents (
    tenant_id,
    subject_id,
    case_id,
    fact_id,
    revision_id,
    projection_schema_version,
    projection_schema_sha256,
    source_content_sha256,
    projection_sha256,
    search_vector
)
SELECT
    revision.tenant_id,
    revision.subject_id,
    revision.case_id,
    revision.fact_id,
    revision.revision_id,
    projection.projection_schema_version,
    projection.projection_sha256,
    revision.content_sha256,
    memory.fact_projection_sha256_v1(
        fact.namespace,
        fact.fact_key,
        revision.value
    ),
    memory.fact_search_vector_v1(
        fact.namespace,
        fact.fact_key,
        revision.value
    )
FROM memory.fact_revisions AS revision
JOIN memory.facts AS fact
  ON fact.tenant_id = revision.tenant_id
 AND fact.subject_id = revision.subject_id
 AND fact.case_id = revision.case_id
 AND fact.fact_id = revision.fact_id
CROSS JOIN memory.search_projection_schemas AS projection
WHERE projection.projection_schema_version = 1
ON CONFLICT DO NOTHING;

CREATE FUNCTION memory.restrict_fact_revision_governance_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'fact revision governance cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF OLD.tenant_id <> NEW.tenant_id
        OR OLD.subject_id <> NEW.subject_id
        OR OLD.case_id <> NEW.case_id
        OR OLD.fact_id <> NEW.fact_id
        OR OLD.revision_id <> NEW.revision_id
        OR OLD.retention_policy_id <> NEW.retention_policy_id
        OR OLD.retention_expires_at IS DISTINCT FROM NEW.retention_expires_at
        OR OLD.recency_profile_id <> NEW.recency_profile_id
        OR OLD.importance <> NEW.importance
        OR OLD.schema_version <> NEW.schema_version
        OR NOT (
            (OLD.lifecycle_state = 'active' AND NEW.lifecycle_state = 'deletion_pending')
            OR (
                OLD.lifecycle_state = 'deletion_pending'
                AND NEW.lifecycle_state = 'deleted'
            )
        )
    THEN
        RAISE EXCEPTION 'invalid fact revision governance transition'
            USING ERRCODE = '55000';
    END IF;

    NEW.lifecycle_changed_at := greatest(
        clock_timestamp(),
        OLD.lifecycle_changed_at + interval '1 microsecond'
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER fact_revision_governance_restrict_mutation
BEFORE UPDATE OR DELETE ON memory.fact_revision_governance
FOR EACH ROW EXECUTE FUNCTION memory.restrict_fact_revision_governance_mutation();

CREATE TRIGGER fact_retention_policies_reject_mutation
BEFORE UPDATE OR DELETE ON memory.fact_retention_policies
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER search_projection_schemas_reject_mutation
BEFORE UPDATE OR DELETE ON memory.search_projection_schemas
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER lexical_retrieval_policies_reject_mutation
BEFORE UPDATE OR DELETE ON memory.lexical_retrieval_policies
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TABLE memory.retrieval_idempotency_reservations (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    idempotency_key text NOT NULL CHECK (
        length(idempotency_key) BETWEEN 1 AND 255
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    retrieval_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    PRIMARY KEY (tenant_id, principal_id, idempotency_key),
    UNIQUE (tenant_id, subject_id, retrieval_id, principal_id)
);

CREATE TRIGGER retrieval_idempotency_reservations_reject_mutation
BEFORE UPDATE OR DELETE ON memory.retrieval_idempotency_reservations
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TABLE memory.retrieval_receipts (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    retrieval_id uuid NOT NULL DEFAULT uuidv7(),
    principal_id text NOT NULL CHECK (btrim(principal_id) <> ''),
    idempotency_key text NOT NULL CHECK (
        length(idempotency_key) BETWEEN 1 AND 255
    ),
    request_fingerprint character(64) NOT NULL CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    query_sha256 character(64) NOT NULL CHECK (
        query_sha256 ~ '^[0-9a-f]{64}$'
    ),
    perspective text NOT NULL CHECK (perspective IN ('current', 'as_of')),
    valid_at timestamptz NOT NULL CHECK (isfinite(valid_at)),
    recorded_at timestamptz NOT NULL CHECK (isfinite(recorded_at)),
    evaluated_at timestamptz NOT NULL CHECK (isfinite(evaluated_at)),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_sha256 character(64) NOT NULL CHECK (
        policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    projection_schema_version integer NOT NULL CHECK (
        projection_schema_version > 0
    ),
    projection_schema_sha256 character(64) NOT NULL CHECK (
        projection_schema_sha256 ~ '^[0-9a-f]{64}$'
    ),
    authorization_scope_sha256 character(64) NOT NULL CHECK (
        authorization_scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    authorization_policy_version text NOT NULL CHECK (
        btrim(authorization_policy_version) <> ''
        AND length(authorization_policy_version) <= 255
    ),
    page_size smallint NOT NULL CHECK (page_size BETWEEN 1 AND 50),
    outcome text NOT NULL CHECK (outcome IN ('results', 'abstention')),
    abstention_reason text CHECK (
        abstention_reason IS NULL
        OR (
            btrim(abstention_reason) <> ''
            AND length(abstention_reason) <= 255
        )
    ),
    stage_timings_ms jsonb NOT NULL CHECK (
        jsonb_typeof(stage_timings_ms) = 'object'
    ),
    manifest_sha256 character(64) NOT NULL CHECK (
        manifest_sha256 ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, retrieval_id),
    UNIQUE (tenant_id, subject_id, retrieval_id, principal_id),
    UNIQUE (tenant_id, principal_id, idempotency_key),
    FOREIGN KEY (tenant_id, subject_id, retrieval_id, principal_id)
        REFERENCES memory.retrieval_idempotency_reservations (
            tenant_id, subject_id, retrieval_id, principal_id
        ),
    FOREIGN KEY (policy_id, policy_version, policy_sha256)
        REFERENCES memory.lexical_retrieval_policies (
            policy_id, policy_version, policy_sha256
        ),
    FOREIGN KEY (projection_schema_version, projection_schema_sha256)
        REFERENCES memory.search_projection_schemas (
            projection_schema_version, projection_sha256
        ),
    CHECK (
        (perspective = 'current' AND valid_at = evaluated_at AND recorded_at = evaluated_at)
        OR (
            perspective = 'as_of'
            AND recorded_at <= evaluated_at
        )
    ),
    CHECK (
        (outcome = 'results' AND abstention_reason IS NULL)
        OR (outcome = 'abstention' AND abstention_reason IS NOT NULL)
    )
);

CREATE TABLE memory.retrieval_manifest_items (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    retrieval_id uuid NOT NULL,
    principal_id text NOT NULL,
    ordinal smallint NOT NULL CHECK (ordinal BETWEEN 1 AND 100),
    cursor_token uuid NOT NULL DEFAULT uuidv7(),
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    exact_identity_rank smallint CHECK (exact_identity_rank > 0),
    lexical_rank bigint CHECK (lexical_rank > 0),
    lexical_score numeric(20, 12) NOT NULL CHECK (lexical_score >= 0),
    final_rank smallint NOT NULL CHECK (final_rank > 0),
    final_score numeric(20, 12) NOT NULL CHECK (final_score >= 0),
    source_content_sha256 character(64) NOT NULL CHECK (
        source_content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    projection_sha256 character(64) NOT NULL CHECK (
        projection_sha256 ~ '^[0-9a-f]{64}$'
    ),
    item_sha256 character(64) NOT NULL CHECK (
        item_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    PRIMARY KEY (tenant_id, subject_id, retrieval_id, ordinal),
    UNIQUE (tenant_id, subject_id, retrieval_id, cursor_token),
    FOREIGN KEY (tenant_id, subject_id, retrieval_id, principal_id)
        REFERENCES memory.retrieval_receipts (
            tenant_id, subject_id, retrieval_id, principal_id
        ),
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id, revision_id)
        REFERENCES memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ),
    CHECK (exact_identity_rank IS NOT NULL OR lexical_rank IS NOT NULL),
    CHECK (final_rank = ordinal)
);

CREATE INDEX retrieval_manifest_items_page_idx
    ON memory.retrieval_manifest_items (
        tenant_id, subject_id, retrieval_id, ordinal
    );

CREATE TRIGGER retrieval_receipts_reject_mutation
BEFORE UPDATE OR DELETE ON memory.retrieval_receipts
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER retrieval_manifest_items_reject_mutation
BEFORE UPDATE OR DELETE ON memory.retrieval_manifest_items
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

ALTER TABLE memory.fact_retention_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_retention_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY fact_retention_policies_select
    ON memory.fact_retention_policies FOR SELECT USING (true);

ALTER TABLE memory.search_projection_schemas ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.search_projection_schemas FORCE ROW LEVEL SECURITY;
CREATE POLICY search_projection_schemas_select
    ON memory.search_projection_schemas FOR SELECT USING (true);

ALTER TABLE memory.lexical_retrieval_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.lexical_retrieval_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY lexical_retrieval_policies_select
    ON memory.lexical_retrieval_policies FOR SELECT USING (true);

ALTER TABLE memory.fact_revision_governance ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_governance FORCE ROW LEVEL SECURITY;
CREATE POLICY fact_revision_governance_select_scope
    ON memory.fact_revision_governance
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY fact_revision_governance_insert_scope
    ON memory.fact_revision_governance
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY fact_revision_governance_update_scope
    ON memory.fact_revision_governance
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.fact_revision_search_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_search_documents FORCE ROW LEVEL SECURITY;
CREATE POLICY fact_revision_search_documents_select_scope
    ON memory.fact_revision_search_documents
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY fact_revision_search_documents_insert_scope
    ON memory.fact_revision_search_documents
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY fact_revision_search_documents_update_scope
    ON memory.fact_revision_search_documents
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );
CREATE POLICY fact_revision_search_documents_delete_scope
    ON memory.fact_revision_search_documents
    FOR DELETE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

ALTER TABLE memory.retrieval_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.retrieval_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY retrieval_receipts_select_scope
    ON memory.retrieval_receipts
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );
CREATE POLICY retrieval_receipts_insert_scope
    ON memory.retrieval_receipts
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );

ALTER TABLE memory.retrieval_idempotency_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.retrieval_idempotency_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY retrieval_idempotency_reservations_select_scope
    ON memory.retrieval_idempotency_reservations
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );
CREATE POLICY retrieval_idempotency_reservations_insert_scope
    ON memory.retrieval_idempotency_reservations
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );

ALTER TABLE memory.retrieval_manifest_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.retrieval_manifest_items FORCE ROW LEVEL SECURITY;
CREATE POLICY retrieval_manifest_items_select_scope
    ON memory.retrieval_manifest_items
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );
CREATE POLICY retrieval_manifest_items_insert_scope
    ON memory.retrieval_manifest_items
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND principal_id = NULLIF(current_setting('palimpsest.principal_id', true), '')
    );

-- Consumers must read manifest items through this view in the same read-only,
-- repeatable-read transaction that rehydrates fact content. It intentionally
-- exposes no memory values and no count of rows hidden by current policy.
CREATE VIEW memory.authorized_retrieval_manifest
WITH (security_barrier = true, security_invoker = true)
AS
SELECT
    item.tenant_id,
    item.subject_id,
    item.retrieval_id,
    item.principal_id,
    item.ordinal,
    item.cursor_token,
    item.case_id,
    item.fact_id,
    item.revision_id,
    item.exact_identity_rank,
    item.lexical_rank,
    item.lexical_score,
    item.final_rank,
    item.final_score,
    item.source_content_sha256,
    item.projection_sha256,
    item.item_sha256,
    item.schema_version
FROM memory.retrieval_manifest_items AS item
JOIN memory.fact_revisions AS revision
  ON revision.tenant_id = item.tenant_id
 AND revision.subject_id = item.subject_id
 AND revision.case_id = item.case_id
 AND revision.fact_id = item.fact_id
 AND revision.revision_id = item.revision_id
JOIN memory.fact_revision_governance AS governance
  ON governance.tenant_id = revision.tenant_id
 AND governance.subject_id = revision.subject_id
 AND governance.case_id = revision.case_id
 AND governance.fact_id = revision.fact_id
 AND governance.revision_id = revision.revision_id
WHERE item.tenant_id = NULLIF(
        current_setting('palimpsest.tenant_id', true),
        ''
    )::uuid
  AND item.subject_id = NULLIF(
        current_setting('palimpsest.subject_id', true),
        ''
    )::uuid
  AND item.principal_id = NULLIF(
        current_setting('palimpsest.principal_id', true),
        ''
    )
  AND governance.lifecycle_state = 'active'
  AND (
      governance.retention_expires_at IS NULL
      OR governance.retention_expires_at > CURRENT_TIMESTAMP
  )
  AND revision.sensitivity IN (
      SELECT allowed.label
      FROM jsonb_array_elements_text(
          COALESCE(
              NULLIF(
                  current_setting('palimpsest.allowed_sensitivities', true),
                  ''
              )::jsonb,
              '[]'::jsonb
          )
      ) AS allowed(label)
  )
  AND item.source_content_sha256 = revision.content_sha256;
