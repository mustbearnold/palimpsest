-- Generalize the pre-release lexical policy registry without changing its
-- existing row or any receipt foreign keys. PostgreSQL follows the renamed
-- relation when preserving those foreign keys.
ALTER TABLE memory.lexical_retrieval_policies
    RENAME TO retrieval_policies;

ALTER TRIGGER lexical_retrieval_policies_reject_mutation
    ON memory.retrieval_policies
    RENAME TO retrieval_policies_reject_mutation;

ALTER POLICY lexical_retrieval_policies_select
    ON memory.retrieval_policies
    RENAME TO retrieval_policies_select;

CREATE TABLE memory.embedding_profiles (
    profile_id text NOT NULL CHECK (
        btrim(profile_id) <> '' AND length(profile_id) <= 255
    ),
    profile_version text NOT NULL CHECK (
        btrim(profile_version) <> '' AND length(profile_version) <= 255
    ),
    provider text NOT NULL CHECK (
        btrim(provider) <> '' AND length(provider) <= 255
    ),
    model text NOT NULL CHECK (
        btrim(model) <> '' AND length(model) <= 255
    ),
    model_revision text NOT NULL CHECK (
        btrim(model_revision) <> ''
        AND length(model_revision) <= 255
        AND model_revision = btrim(model_revision)
        AND lower(btrim(model_revision)) NOT IN ('latest', 'current', 'stable')
    ),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 16000),
    normalization text NOT NULL CHECK (normalization = 'unit_l2'),
    normalization_tolerance numeric(12, 9) NOT NULL CHECK (
        normalization_tolerance > 0 AND normalization_tolerance < 1
    ),
    distance_metric text NOT NULL CHECK (distance_metric = 'cosine'),
    scalar_type text NOT NULL CHECK (scalar_type = 'float32'),
    input_serialization text NOT NULL CHECK (
        btrim(input_serialization) <> ''
        AND length(input_serialization) <= 255
    ),
    query_task_mode text NOT NULL CHECK (
        btrim(query_task_mode) <> '' AND length(query_task_mode) <= 255
    ),
    document_task_mode text NOT NULL CHECK (
        btrim(document_task_mode) <> ''
        AND length(document_task_mode) <= 255
    ),
    provider_contract_schema_version integer NOT NULL CHECK (
        provider_contract_schema_version > 0
    ),
    profile_document jsonb NOT NULL CHECK (
        jsonb_typeof(profile_document) = 'object'
    ),
    profile_sha256 character(64) NOT NULL CHECK (
        profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    PRIMARY KEY (profile_id, profile_version),
    UNIQUE (profile_id, profile_version, profile_sha256),
    UNIQUE (
        profile_id,
        profile_version,
        profile_sha256,
        dimensions,
        normalization,
        normalization_tolerance,
        distance_metric
    )
);

CREATE TABLE memory.embedding_projection_profiles (
    projection_profile_id text NOT NULL CHECK (
        btrim(projection_profile_id) <> ''
        AND length(projection_profile_id) <= 255
    ),
    projection_profile_version text NOT NULL CHECK (
        btrim(projection_profile_version) <> ''
        AND length(projection_profile_version) <= 255
    ),
    embedding_profile_id text NOT NULL,
    embedding_profile_version text NOT NULL,
    embedding_profile_sha256 character(64) NOT NULL CHECK (
        embedding_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    source_projection_schema_version integer NOT NULL CHECK (
        source_projection_schema_version > 0
    ),
    source_projection_schema_sha256 character(64) NOT NULL CHECK (
        source_projection_schema_sha256 ~ '^[0-9a-f]{64}$'
    ),
    input_serialization text NOT NULL CHECK (
        btrim(input_serialization) <> ''
        AND length(input_serialization) <= 255
    ),
    input_schema_version integer NOT NULL CHECK (input_schema_version > 0),
    projection_document jsonb NOT NULL CHECK (
        jsonb_typeof(projection_document) = 'object'
    ),
    projection_profile_sha256 character(64) NOT NULL CHECK (
        projection_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    PRIMARY KEY (projection_profile_id, projection_profile_version),
    UNIQUE (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256
    ),
    UNIQUE (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ),
    UNIQUE (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256
    ),
    FOREIGN KEY (
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ) REFERENCES memory.embedding_profiles (
        profile_id, profile_version, profile_sha256
    ),
    FOREIGN KEY (
        source_projection_schema_version,
        source_projection_schema_sha256
    ) REFERENCES memory.search_projection_schemas (
        projection_schema_version, projection_sha256
    )
);

CREATE FUNCTION memory.validate_embedding_profile_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    IF NEW.profile_sha256 <> encode(
        sha256(convert_to(NEW.profile_document::text, 'UTF8')),
        'hex'
    )
        OR NEW.profile_document ->> 'provider' IS DISTINCT FROM NEW.provider
        OR NEW.profile_document ->> 'model' IS DISTINCT FROM NEW.model
        OR NEW.profile_document ->> 'model_revision'
            IS DISTINCT FROM NEW.model_revision
        OR (NEW.profile_document ->> 'dimensions')::integer
            IS DISTINCT FROM NEW.dimensions
        OR NEW.profile_document #>> '{normalization,kind}'
            IS DISTINCT FROM NEW.normalization
        OR (NEW.profile_document #>> '{normalization,tolerance}')::numeric
            IS DISTINCT FROM NEW.normalization_tolerance
        OR NEW.profile_document ->> 'distance_metric'
            IS DISTINCT FROM NEW.distance_metric
        OR NEW.profile_document ->> 'scalar_type'
            IS DISTINCT FROM NEW.scalar_type
        OR NEW.profile_document ->> 'serialization'
            IS DISTINCT FROM NEW.input_serialization
        OR NEW.profile_document #>> '{task_modes,query}'
            IS DISTINCT FROM NEW.query_task_mode
        OR NEW.profile_document #>> '{task_modes,document}'
            IS DISTINCT FROM NEW.document_task_mode
        OR (NEW.profile_document ->> 'provider_contract_schema_version')::integer
            IS DISTINCT FROM NEW.provider_contract_schema_version
        OR (NEW.profile_document ->> 'schema_version')::integer
            IS DISTINCT FROM NEW.schema_version
    THEN
        RAISE EXCEPTION 'embedding profile registration is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_profile_registration_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.validate_embedding_projection_profile_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    IF NEW.projection_profile_sha256 <> encode(
        sha256(convert_to(NEW.projection_document::text, 'UTF8')),
        'hex'
    )
        OR NEW.projection_document #>> '{embedding_profile,id}'
            IS DISTINCT FROM NEW.embedding_profile_id
        OR NEW.projection_document #>> '{embedding_profile,version}'
            IS DISTINCT FROM NEW.embedding_profile_version
        OR NEW.projection_document #>> '{embedding_profile,digest}'
            IS DISTINCT FROM NEW.embedding_profile_sha256
        OR (NEW.projection_document #>> '{source_projection,schema_version}')::integer
            IS DISTINCT FROM NEW.source_projection_schema_version
        OR NEW.projection_document #>> '{source_projection,digest}'
            IS DISTINCT FROM NEW.source_projection_schema_sha256
        OR NEW.projection_document ->> 'serialization'
            IS DISTINCT FROM NEW.input_serialization
        OR (NEW.projection_document ->> 'input_schema_version')::integer
            IS DISTINCT FROM NEW.input_schema_version
        OR (NEW.projection_document ->> 'schema_version')::integer
            IS DISTINCT FROM NEW.schema_version
    THEN
        RAISE EXCEPTION 'embedding projection profile registration is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_profile_registration_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER embedding_profiles_validate_registration
BEFORE INSERT ON memory.embedding_profiles
FOR EACH ROW EXECUTE FUNCTION memory.validate_embedding_profile_registration();

CREATE TRIGGER embedding_projection_profiles_validate_registration
BEFORE INSERT ON memory.embedding_projection_profiles
FOR EACH ROW
EXECUTE FUNCTION memory.validate_embedding_projection_profile_registration();

CREATE TRIGGER embedding_profiles_reject_mutation
BEFORE UPDATE OR DELETE ON memory.embedding_profiles
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

CREATE TRIGGER embedding_projection_profiles_reject_mutation
BEFORE UPDATE OR DELETE ON memory.embedding_projection_profiles
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

ALTER TABLE memory.embedding_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.embedding_profiles FORCE ROW LEVEL SECURITY;
CREATE POLICY embedding_profiles_select
    ON memory.embedding_profiles FOR SELECT USING (true);

ALTER TABLE memory.embedding_projection_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.embedding_projection_profiles FORCE ROW LEVEL SECURITY;
CREATE POLICY embedding_projection_profiles_select
    ON memory.embedding_projection_profiles FOR SELECT USING (true);

-- The policy row owns the complete provider/projection plan. The existing
-- lexical policy has no embedding lineage; future hybrid policies must bind
-- both registries atomically.
ALTER TABLE memory.retrieval_policies
    ADD COLUMN retrieval_mode text NOT NULL DEFAULT 'lexical'
        CHECK (retrieval_mode IN ('lexical', 'hybrid')),
    ADD COLUMN embedding_profile_id text,
    ADD COLUMN embedding_profile_version text,
    ADD COLUMN embedding_profile_sha256 character(64) CHECK (
        embedding_profile_sha256 IS NULL
        OR embedding_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN embedding_projection_profile_id text,
    ADD COLUMN embedding_projection_profile_version text,
    ADD COLUMN embedding_projection_profile_sha256 character(64) CHECK (
        embedding_projection_profile_sha256 IS NULL
        OR embedding_projection_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT retrieval_policies_embedding_lineage_complete CHECK (
        (
            retrieval_mode = 'lexical'
            AND embedding_profile_id IS NULL
            AND embedding_profile_version IS NULL
            AND embedding_profile_sha256 IS NULL
            AND embedding_projection_profile_id IS NULL
            AND embedding_projection_profile_version IS NULL
            AND embedding_projection_profile_sha256 IS NULL
        )
        OR (
            retrieval_mode = 'hybrid'
            AND embedding_profile_id IS NOT NULL
            AND embedding_profile_version IS NOT NULL
            AND embedding_profile_sha256 IS NOT NULL
            AND embedding_projection_profile_id IS NOT NULL
            AND embedding_projection_profile_version IS NOT NULL
            AND embedding_projection_profile_sha256 IS NOT NULL
        )
    ),
    ADD FOREIGN KEY (
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ) REFERENCES memory.embedding_profiles (
        profile_id, profile_version, profile_sha256
    ),
    ADD FOREIGN KEY (
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ) REFERENCES memory.embedding_projection_profiles (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    );

CREATE FUNCTION memory.validate_retrieval_policy_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    IF NEW.policy_sha256 <> encode(
        sha256(convert_to(NEW.policy_document::text, 'UTF8')),
        'hex'
    )
        OR (
            NEW.retrieval_mode = 'hybrid'
            AND (
                NEW.policy_document #>> '{embedding_profile,id}'
                    IS DISTINCT FROM NEW.embedding_profile_id
                OR NEW.policy_document #>> '{embedding_profile,version}'
                    IS DISTINCT FROM NEW.embedding_profile_version
                OR NEW.policy_document #>> '{embedding_profile,digest}'
                    IS DISTINCT FROM NEW.embedding_profile_sha256
                OR NEW.policy_document #>> '{projection_profile,id}'
                    IS DISTINCT FROM NEW.embedding_projection_profile_id
                OR NEW.policy_document #>> '{projection_profile,version}'
                    IS DISTINCT FROM NEW.embedding_projection_profile_version
                OR NEW.policy_document #>> '{projection_profile,digest}'
                    IS DISTINCT FROM NEW.embedding_projection_profile_sha256
            )
        )
    THEN
        RAISE EXCEPTION 'retrieval policy registration is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'retrieval_policy_registration_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER retrieval_policies_validate_registration
BEFORE INSERT ON memory.retrieval_policies
FOR EACH ROW EXECUTE FUNCTION memory.validate_retrieval_policy_registration();

ALTER TABLE memory.fact_revisions
    ADD CONSTRAINT fact_revisions_embedding_source_key UNIQUE (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        content_sha256
    );

ALTER TABLE memory.fact_revision_search_documents
    ADD CONSTRAINT fact_revision_search_documents_embedding_source_key UNIQUE (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        source_content_sha256,
        projection_schema_version,
        projection_schema_sha256,
        projection_sha256
    );

CREATE TABLE memory.fact_revision_embedding_projections (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    case_id uuid NOT NULL,
    fact_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    embedding_profile_id text NOT NULL,
    embedding_profile_version text NOT NULL,
    embedding_profile_sha256 character(64) NOT NULL CHECK (
        embedding_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    embedding_dimensions integer NOT NULL CHECK (
        embedding_dimensions BETWEEN 1 AND 16000
    ),
    normalization text NOT NULL CHECK (normalization = 'unit_l2'),
    normalization_tolerance numeric(12, 9) NOT NULL CHECK (
        normalization_tolerance > 0 AND normalization_tolerance < 1
    ),
    distance_metric text NOT NULL CHECK (distance_metric = 'cosine'),
    embedding_projection_profile_id text NOT NULL,
    embedding_projection_profile_version text NOT NULL,
    embedding_projection_profile_sha256 character(64) NOT NULL CHECK (
        embedding_projection_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    source_projection_schema_version integer NOT NULL CHECK (
        source_projection_schema_version > 0
    ),
    source_projection_schema_sha256 character(64) NOT NULL CHECK (
        source_projection_schema_sha256 ~ '^[0-9a-f]{64}$'
    ),
    source_content_sha256 character(64) NOT NULL CHECK (
        source_content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    source_projection_sha256 character(64) NOT NULL CHECK (
        source_projection_sha256 ~ '^[0-9a-f]{64}$'
    ),
    input_sha256 character(64) NOT NULL CHECK (
        input_sha256 ~ '^[0-9a-f]{64}$'
    ),
    status text NOT NULL CHECK (
        status IN ('pending', 'generating', 'ready', 'failed')
    ),
    embedding vector,
    vector_sha256 character(64) CHECK (
        vector_sha256 IS NULL OR vector_sha256 ~ '^[0-9a-f]{64}$'
    ),
    failure_code text CHECK (
        failure_code IS NULL
        OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    generation_attempt_id uuid,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(queued_at)),
    generation_started_at timestamptz CHECK (
        generation_started_at IS NULL OR isfinite(generation_started_at)
    ),
    generated_at timestamptz CHECK (
        generated_at IS NULL OR isfinite(generated_at)
    ),
    status_changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(status_changed_at)),
    generation_schema_version integer NOT NULL CHECK (
        generation_schema_version > 0
    ),
    PRIMARY KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        embedding_profile_id,
        embedding_profile_version,
        embedding_projection_profile_id,
        embedding_projection_profile_version
    ),
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        source_content_sha256
    ) REFERENCES memory.fact_revisions (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        content_sha256
    ),
    FOREIGN KEY (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        source_content_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256,
        source_projection_sha256
    ) REFERENCES memory.fact_revision_search_documents (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        source_content_sha256,
        projection_schema_version,
        projection_schema_sha256,
        projection_sha256
    ) ON DELETE CASCADE,
    FOREIGN KEY (
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        embedding_dimensions,
        normalization,
        normalization_tolerance,
        distance_metric
    ) REFERENCES memory.embedding_profiles (
        profile_id,
        profile_version,
        profile_sha256,
        dimensions,
        normalization,
        normalization_tolerance,
        distance_metric
    ),
    FOREIGN KEY (
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256
    ) REFERENCES memory.embedding_projection_profiles (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256
    )
);

CREATE INDEX fact_revision_embedding_projections_queue_idx
    ON memory.fact_revision_embedding_projections (
        tenant_id,
        subject_id,
        status,
        embedding_profile_id,
        embedding_profile_version,
        queued_at,
        revision_id
    );

CREATE FUNCTION memory.embedding_vector_sha256_v1(
    embedding_value public.vector,
    embedding_dimensions integer
)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $$
    SELECT encode(
        sha256(
            convert_to('palimpsest.embedding.float32-be.v1', 'UTF8')
            || decode('00', 'hex')
            || COALESCE(
                string_agg(
                    float4send(element.value),
                    ''::bytea
                    ORDER BY element.ordinality
                ),
                ''::bytea
            )
        ),
        'hex'
    )
    FROM unnest(
        vector_to_float4(embedding_value, embedding_dimensions, false)
    ) WITH ORDINALITY AS element(value, ordinality)
$$;

CREATE FUNCTION memory.validate_fact_revision_embedding_projection()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, memory
AS $$
DECLARE
    embedding_norm double precision;
BEGIN
    -- Validate every digest before table constraints can include the row in an
    -- error detail. Projection failures expose only stable redacted codes.
    IF NEW.embedding_profile_sha256 !~ '^[0-9a-f]{64}$'
        OR NEW.embedding_projection_profile_sha256 !~ '^[0-9a-f]{64}$'
        OR NEW.source_projection_schema_sha256 !~ '^[0-9a-f]{64}$'
        OR NEW.source_content_sha256 !~ '^[0-9a-f]{64}$'
        OR NEW.source_projection_sha256 !~ '^[0-9a-f]{64}$'
        OR NEW.input_sha256 !~ '^[0-9a-f]{64}$'
        OR (
            NEW.vector_sha256 IS NOT NULL
            AND NEW.vector_sha256 !~ '^[0-9a-f]{64}$'
        )
    THEN
        RAISE EXCEPTION 'embedding projection lineage is invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_lineage_valid';
    END IF;

    IF TG_OP = 'UPDATE' AND (
        OLD.tenant_id <> NEW.tenant_id
        OR OLD.subject_id <> NEW.subject_id
        OR OLD.case_id <> NEW.case_id
        OR OLD.fact_id <> NEW.fact_id
        OR OLD.revision_id <> NEW.revision_id
        OR OLD.embedding_profile_id <> NEW.embedding_profile_id
        OR OLD.embedding_profile_version <> NEW.embedding_profile_version
        OR OLD.embedding_profile_sha256 <> NEW.embedding_profile_sha256
        OR OLD.embedding_dimensions <> NEW.embedding_dimensions
        OR OLD.normalization <> NEW.normalization
        OR OLD.normalization_tolerance <> NEW.normalization_tolerance
        OR OLD.distance_metric <> NEW.distance_metric
        OR OLD.embedding_projection_profile_id
            <> NEW.embedding_projection_profile_id
        OR OLD.embedding_projection_profile_version
            <> NEW.embedding_projection_profile_version
        OR OLD.embedding_projection_profile_sha256
            <> NEW.embedding_projection_profile_sha256
        OR OLD.source_projection_schema_version
            <> NEW.source_projection_schema_version
        OR OLD.source_projection_schema_sha256
            <> NEW.source_projection_schema_sha256
        OR OLD.source_content_sha256 <> NEW.source_content_sha256
        OR OLD.source_projection_sha256 <> NEW.source_projection_sha256
        OR OLD.input_sha256 <> NEW.input_sha256
        OR OLD.queued_at <> NEW.queued_at
        OR OLD.generation_schema_version <> NEW.generation_schema_version
    ) THEN
        RAISE EXCEPTION 'embedding projection lineage is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.status = 'pending' AND (
        NEW.embedding IS NOT NULL
        OR NEW.vector_sha256 IS NOT NULL
        OR NEW.failure_code IS NOT NULL
        OR NEW.generation_attempt_id IS NOT NULL
        OR NEW.generation_started_at IS NOT NULL
        OR NEW.generated_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'pending embedding projection state is invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_pending_state_valid';
    ELSIF NEW.status = 'generating' AND (
        NEW.embedding IS NOT NULL
        OR NEW.vector_sha256 IS NOT NULL
        OR NEW.failure_code IS NOT NULL
        OR NEW.generation_attempt_id IS NULL
        OR NEW.generation_started_at IS NULL
        OR NEW.generated_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'generating embedding projection state is invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_generating_state_valid';
    ELSIF NEW.status = 'failed' AND (
        NEW.embedding IS NOT NULL
        OR NEW.vector_sha256 IS NOT NULL
        OR NEW.failure_code IS NULL
        OR NEW.failure_code !~ '^[a-z][a-z0-9_]{0,63}$'
        OR NEW.generation_attempt_id IS NULL
        OR NEW.generation_started_at IS NULL
        OR NEW.generated_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'failed embedding projection state is invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_failed_state_valid';
    ELSIF NEW.status = 'ready' AND (
        NEW.embedding IS NULL
        OR NEW.vector_sha256 IS NULL
        OR NEW.failure_code IS NOT NULL
        OR NEW.generation_attempt_id IS NULL
        OR NEW.generation_started_at IS NULL
        OR NEW.generated_at IS NULL
    ) THEN
        RAISE EXCEPTION 'ready embedding projection state is invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_ready_state_valid';
    END IF;

    IF NEW.generation_started_at IS NOT NULL
        AND NEW.generation_started_at < NEW.queued_at
    THEN
        RAISE EXCEPTION 'embedding projection timestamps are invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_timestamps_valid';
    END IF;

    IF NEW.generated_at IS NOT NULL AND (
        NEW.generation_started_at IS NULL
        OR NEW.generated_at < NEW.generation_started_at
    ) THEN
        RAISE EXCEPTION 'embedding projection timestamps are invalid'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'embedding_projection_timestamps_valid';
    END IF;

    IF NEW.embedding IS NOT NULL THEN
        IF vector_dims(NEW.embedding) <> NEW.embedding_dimensions THEN
            RAISE EXCEPTION 'embedding projection dimension mismatch'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'embedding_projection_dimensions_match';
        END IF;

        embedding_norm := vector_norm(NEW.embedding);
        IF embedding_norm = 'NaN'::double precision
            OR embedding_norm = 'Infinity'::double precision
            OR embedding_norm <= 0
        THEN
            RAISE EXCEPTION 'embedding projection norm is invalid'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'embedding_projection_norm_valid';
        END IF;

        IF NEW.normalization = 'unit_l2'
            AND abs(embedding_norm - 1.0)
                > NEW.normalization_tolerance::double precision
        THEN
            RAISE EXCEPTION 'embedding projection normalization mismatch'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'embedding_projection_normalized';
        END IF;

        IF NEW.vector_sha256 IS DISTINCT FROM
            memory.embedding_vector_sha256_v1(
                NEW.embedding,
                NEW.embedding_dimensions
            )
        THEN
            RAISE EXCEPTION 'embedding projection digest mismatch'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'embedding_projection_digest_valid';
        END IF;
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status THEN
        NEW.status_changed_at := greatest(
            clock_timestamp(),
            OLD.status_changed_at + interval '1 microsecond'
        );
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER fact_revision_embedding_projections_validate
BEFORE INSERT OR UPDATE ON memory.fact_revision_embedding_projections
FOR EACH ROW EXECUTE FUNCTION memory.validate_fact_revision_embedding_projection();

ALTER TABLE memory.fact_revision_embedding_projections
    ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_embedding_projections
    FORCE ROW LEVEL SECURITY;

CREATE POLICY fact_revision_embedding_projections_select_scope
    ON memory.fact_revision_embedding_projections
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

CREATE POLICY fact_revision_embedding_projections_insert_scope
    ON memory.fact_revision_embedding_projections
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

CREATE POLICY fact_revision_embedding_projections_update_scope
    ON memory.fact_revision_embedding_projections
    FOR UPDATE
    USING (
        tenant_id = NULLIF(
            current_setting('palimpsest.tenant_id', true),
            ''
        )::uuid
        AND subject_id = NULLIF(
            current_setting('palimpsest.subject_id', true),
            ''
        )::uuid
    )
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

CREATE POLICY fact_revision_embedding_projections_delete_scope
    ON memory.fact_revision_embedding_projections
    FOR DELETE
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

-- Install the document trigger before any profile can be registered. A later
-- profile registration locks document inserts while its AFTER trigger queues
-- the existing corpus; writers resume only after the profile is visible.
CREATE FUNCTION memory.queue_fact_revision_embedding_projections()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    INSERT INTO memory.fact_revision_embedding_projections (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        embedding_dimensions,
        normalization,
        normalization_tolerance,
        distance_metric,
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256,
        source_content_sha256,
        source_projection_sha256,
        input_sha256,
        status,
        generation_schema_version
    )
    SELECT
        NEW.tenant_id,
        NEW.subject_id,
        NEW.case_id,
        NEW.fact_id,
        NEW.revision_id,
        embedding.profile_id,
        embedding.profile_version,
        embedding.profile_sha256,
        embedding.dimensions,
        embedding.normalization,
        embedding.normalization_tolerance,
        embedding.distance_metric,
        projection.projection_profile_id,
        projection.projection_profile_version,
        projection.projection_profile_sha256,
        NEW.projection_schema_version,
        NEW.projection_schema_sha256,
        NEW.source_content_sha256,
        NEW.projection_sha256,
        NEW.projection_sha256,
        'pending',
        1
    FROM memory.embedding_projection_profiles AS projection
    JOIN memory.embedding_profiles AS embedding
      ON embedding.profile_id = projection.embedding_profile_id
     AND embedding.profile_version = projection.embedding_profile_version
     AND embedding.profile_sha256 = projection.embedding_profile_sha256
    WHERE projection.source_projection_schema_version
            = NEW.projection_schema_version
      AND projection.source_projection_schema_sha256
            = NEW.projection_schema_sha256
    ON CONFLICT DO NOTHING;

    RETURN NULL;
END;
$$;

CREATE TRIGGER fact_revision_search_documents_queue_embeddings
AFTER INSERT ON memory.fact_revision_search_documents
FOR EACH ROW EXECUTE FUNCTION memory.queue_fact_revision_embedding_projections();

CREATE FUNCTION memory.lock_embedding_projection_profile_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    LOCK TABLE memory.fact_revision_search_documents
        IN SHARE ROW EXCLUSIVE MODE;
    RETURN NEW;
END;
$$;

CREATE TRIGGER embedding_projection_profiles_lock_registration
BEFORE INSERT ON memory.embedding_projection_profiles
FOR EACH ROW
EXECUTE FUNCTION memory.lock_embedding_projection_profile_registration();

CREATE FUNCTION memory.backfill_embedding_projection_profile()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    INSERT INTO memory.fact_revision_embedding_projections (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        embedding_dimensions,
        normalization,
        normalization_tolerance,
        distance_metric,
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256,
        source_content_sha256,
        source_projection_sha256,
        input_sha256,
        status,
        generation_schema_version
    )
    SELECT
        document.tenant_id,
        document.subject_id,
        document.case_id,
        document.fact_id,
        document.revision_id,
        embedding.profile_id,
        embedding.profile_version,
        embedding.profile_sha256,
        embedding.dimensions,
        embedding.normalization,
        embedding.normalization_tolerance,
        embedding.distance_metric,
        NEW.projection_profile_id,
        NEW.projection_profile_version,
        NEW.projection_profile_sha256,
        document.projection_schema_version,
        document.projection_schema_sha256,
        document.source_content_sha256,
        document.projection_sha256,
        document.projection_sha256,
        'pending',
        1
    FROM memory.fact_revision_search_documents AS document
    JOIN memory.embedding_profiles AS embedding
      ON embedding.profile_id = NEW.embedding_profile_id
     AND embedding.profile_version = NEW.embedding_profile_version
     AND embedding.profile_sha256 = NEW.embedding_profile_sha256
    WHERE document.projection_schema_version
            = NEW.source_projection_schema_version
      AND document.projection_schema_sha256
            = NEW.source_projection_schema_sha256
    ON CONFLICT DO NOTHING;

    RETURN NULL;
END;
$$;

CREATE TRIGGER embedding_projection_profiles_backfill
AFTER INSERT ON memory.embedding_projection_profiles
FOR EACH ROW EXECUTE FUNCTION memory.backfill_embedding_projection_profile();

-- A scoped coordinator calls this before claiming pending work. It recreates
-- rows removed during a derived-projection rebuild without granting any
-- cross-tenant visibility or bypassing the caller's forced-RLS scope.
CREATE FUNCTION memory.enqueue_missing_fact_revision_embedding_projections()
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    inserted_count bigint;
BEGIN
    INSERT INTO memory.fact_revision_embedding_projections (
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        revision_id,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256,
        embedding_dimensions,
        normalization,
        normalization_tolerance,
        distance_metric,
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        source_projection_schema_version,
        source_projection_schema_sha256,
        source_content_sha256,
        source_projection_sha256,
        input_sha256,
        status,
        generation_schema_version
    )
    SELECT
        document.tenant_id,
        document.subject_id,
        document.case_id,
        document.fact_id,
        document.revision_id,
        embedding.profile_id,
        embedding.profile_version,
        embedding.profile_sha256,
        embedding.dimensions,
        embedding.normalization,
        embedding.normalization_tolerance,
        embedding.distance_metric,
        projection.projection_profile_id,
        projection.projection_profile_version,
        projection.projection_profile_sha256,
        document.projection_schema_version,
        document.projection_schema_sha256,
        document.source_content_sha256,
        document.projection_sha256,
        document.projection_sha256,
        'pending',
        1
    FROM memory.fact_revision_search_documents AS document
    JOIN memory.embedding_projection_profiles AS projection
      ON projection.source_projection_schema_version
            = document.projection_schema_version
     AND projection.source_projection_schema_sha256
            = document.projection_schema_sha256
    JOIN memory.embedding_profiles AS embedding
      ON embedding.profile_id = projection.embedding_profile_id
     AND embedding.profile_version = projection.embedding_profile_version
     AND embedding.profile_sha256 = projection.embedding_profile_sha256
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

-- This is the sole internal vector read seam. Retrieval must first materialize
-- and authorize effective revisions, then join those revision IDs to this
-- security-invoker view. The view verifies projection lineage but deliberately
-- does not perform bitemporal selection or sensitivity authorization itself.
CREATE VIEW memory.retrieval_ready_fact_revision_embeddings
WITH (security_barrier = true, security_invoker = true)
AS
SELECT
    projection.tenant_id,
    projection.subject_id,
    projection.case_id,
    projection.fact_id,
    projection.revision_id,
    projection.embedding_profile_id,
    projection.embedding_profile_version,
    projection.embedding_profile_sha256,
    projection.embedding_dimensions,
    projection.embedding_projection_profile_id,
    projection.embedding_projection_profile_version,
    projection.embedding_projection_profile_sha256,
    projection.source_projection_schema_version,
    projection.source_projection_schema_sha256,
    projection.source_content_sha256,
    projection.source_projection_sha256,
    projection.input_sha256,
    projection.embedding,
    projection.vector_sha256,
    projection.generated_at,
    projection.generation_schema_version
FROM memory.fact_revision_embedding_projections AS projection
JOIN memory.fact_revisions AS revision
  ON revision.tenant_id = projection.tenant_id
 AND revision.subject_id = projection.subject_id
 AND revision.case_id = projection.case_id
 AND revision.fact_id = projection.fact_id
 AND revision.revision_id = projection.revision_id
 AND revision.content_sha256 = projection.source_content_sha256
JOIN memory.fact_revision_search_documents AS document
  ON document.tenant_id = projection.tenant_id
 AND document.subject_id = projection.subject_id
 AND document.case_id = projection.case_id
 AND document.fact_id = projection.fact_id
 AND document.revision_id = projection.revision_id
 AND document.source_content_sha256 = projection.source_content_sha256
 AND document.projection_schema_version
        = projection.source_projection_schema_version
 AND document.projection_schema_sha256
        = projection.source_projection_schema_sha256
 AND document.projection_sha256 = projection.source_projection_sha256
JOIN memory.embedding_profiles AS embedding_profile
  ON embedding_profile.profile_id = projection.embedding_profile_id
 AND embedding_profile.profile_version = projection.embedding_profile_version
 AND embedding_profile.profile_sha256 = projection.embedding_profile_sha256
 AND embedding_profile.dimensions = projection.embedding_dimensions
 AND embedding_profile.normalization = projection.normalization
 AND embedding_profile.normalization_tolerance
        = projection.normalization_tolerance
 AND embedding_profile.distance_metric = projection.distance_metric
JOIN memory.embedding_projection_profiles AS projection_profile
  ON projection_profile.projection_profile_id
        = projection.embedding_projection_profile_id
 AND projection_profile.projection_profile_version
        = projection.embedding_projection_profile_version
 AND projection_profile.projection_profile_sha256
        = projection.embedding_projection_profile_sha256
 AND projection_profile.embedding_profile_id
        = projection.embedding_profile_id
 AND projection_profile.embedding_profile_version
        = projection.embedding_profile_version
 AND projection_profile.embedding_profile_sha256
        = projection.embedding_profile_sha256
 AND projection_profile.source_projection_schema_version
        = projection.source_projection_schema_version
 AND projection_profile.source_projection_schema_sha256
        = projection.source_projection_schema_sha256
WHERE projection.status = 'ready'
  AND projection.input_sha256 = document.projection_sha256
  AND vector_dims(projection.embedding) = projection.embedding_dimensions
  AND vector_norm(projection.embedding) > 0
  AND abs(vector_norm(projection.embedding) - 1.0)
        <= projection.normalization_tolerance::double precision
  AND projection.vector_sha256 = memory.embedding_vector_sha256_v1(
      projection.embedding,
      projection.embedding_dimensions
  );

ALTER TABLE memory.retrieval_receipts
    ADD COLUMN embedding_profile_id text,
    ADD COLUMN embedding_profile_version text,
    ADD COLUMN embedding_profile_sha256 character(64) CHECK (
        embedding_profile_sha256 IS NULL
        OR embedding_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN embedding_projection_profile_id text,
    ADD COLUMN embedding_projection_profile_version text,
    ADD COLUMN embedding_projection_profile_sha256 character(64) CHECK (
        embedding_projection_profile_sha256 IS NULL
        OR embedding_projection_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN query_input_sha256 character(64) CHECK (
        query_input_sha256 IS NULL OR query_input_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN query_vector_sha256 character(64) CHECK (
        query_vector_sha256 IS NULL OR query_vector_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT retrieval_receipts_embedding_lineage_complete CHECK (
        (
            embedding_profile_id IS NULL
            AND embedding_profile_version IS NULL
            AND embedding_profile_sha256 IS NULL
            AND embedding_projection_profile_id IS NULL
            AND embedding_projection_profile_version IS NULL
            AND embedding_projection_profile_sha256 IS NULL
            AND query_input_sha256 IS NULL
            AND query_vector_sha256 IS NULL
        )
        OR (
            embedding_profile_id IS NOT NULL
            AND embedding_profile_version IS NOT NULL
            AND embedding_profile_sha256 IS NOT NULL
            AND embedding_projection_profile_id IS NOT NULL
            AND embedding_projection_profile_version IS NOT NULL
            AND embedding_projection_profile_sha256 IS NOT NULL
            AND query_input_sha256 IS NOT NULL
            AND query_vector_sha256 IS NOT NULL
        )
    ),
    ADD FOREIGN KEY (
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ) REFERENCES memory.embedding_profiles (
        profile_id, profile_version, profile_sha256
    ),
    ADD FOREIGN KEY (
        embedding_projection_profile_id,
        embedding_projection_profile_version,
        embedding_projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    ) REFERENCES memory.embedding_projection_profiles (
        projection_profile_id,
        projection_profile_version,
        projection_profile_sha256,
        embedding_profile_id,
        embedding_profile_version,
        embedding_profile_sha256
    );

CREATE FUNCTION memory.validate_retrieval_receipt_embedding_lineage()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    planned_mode text;
    planned_embedding_profile_id text;
    planned_embedding_profile_version text;
    planned_embedding_profile_sha256 character(64);
    planned_projection_profile_id text;
    planned_projection_profile_version text;
    planned_projection_profile_sha256 character(64);
BEGIN
    SELECT
        policy.retrieval_mode,
        policy.embedding_profile_id,
        policy.embedding_profile_version,
        policy.embedding_profile_sha256,
        policy.embedding_projection_profile_id,
        policy.embedding_projection_profile_version,
        policy.embedding_projection_profile_sha256
    INTO
        planned_mode,
        planned_embedding_profile_id,
        planned_embedding_profile_version,
        planned_embedding_profile_sha256,
        planned_projection_profile_id,
        planned_projection_profile_version,
        planned_projection_profile_sha256
    FROM memory.retrieval_policies AS policy
    WHERE policy.policy_id = NEW.policy_id
      AND policy.policy_version = NEW.policy_version
      AND policy.policy_sha256 = NEW.policy_sha256;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'retrieval policy is unavailable'
            USING ERRCODE = '23503';
    END IF;

    IF planned_mode = 'lexical' AND (
        NEW.embedding_profile_id IS NOT NULL
        OR NEW.embedding_profile_version IS NOT NULL
        OR NEW.embedding_profile_sha256 IS NOT NULL
        OR NEW.embedding_projection_profile_id IS NOT NULL
        OR NEW.embedding_projection_profile_version IS NOT NULL
        OR NEW.embedding_projection_profile_sha256 IS NOT NULL
        OR NEW.query_input_sha256 IS NOT NULL
        OR NEW.query_vector_sha256 IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'lexical receipt cannot contain embedding lineage'
            USING ERRCODE = '23514';
    ELSIF planned_mode = 'hybrid' AND (
        NEW.embedding_profile_id IS DISTINCT FROM planned_embedding_profile_id
        OR NEW.embedding_profile_version
            IS DISTINCT FROM planned_embedding_profile_version
        OR NEW.embedding_profile_sha256
            IS DISTINCT FROM planned_embedding_profile_sha256
        OR NEW.embedding_projection_profile_id
            IS DISTINCT FROM planned_projection_profile_id
        OR NEW.embedding_projection_profile_version
            IS DISTINCT FROM planned_projection_profile_version
        OR NEW.embedding_projection_profile_sha256
            IS DISTINCT FROM planned_projection_profile_sha256
        OR NEW.query_input_sha256 IS NULL
        OR NEW.query_vector_sha256 IS NULL
    ) THEN
        RAISE EXCEPTION 'hybrid receipt embedding lineage is invalid'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER retrieval_receipts_validate_embedding_lineage
BEFORE INSERT ON memory.retrieval_receipts
FOR EACH ROW EXECUTE FUNCTION memory.validate_retrieval_receipt_embedding_lineage();

ALTER TABLE memory.retrieval_manifest_items
    ADD COLUMN exact_rank smallint CHECK (exact_rank > 0),
    ADD COLUMN vector_rank smallint CHECK (vector_rank > 0),
    ADD COLUMN vector_distance numeric(20, 12) CHECK (
        vector_distance IS NULL OR vector_distance BETWEEN 0 AND 2
    ),
    ADD COLUMN vector_similarity numeric(20, 12) CHECK (
        vector_similarity IS NULL OR vector_similarity BETWEEN -1 AND 1
    ),
    ADD COLUMN exact_rrf_contribution numeric(20, 12) NOT NULL DEFAULT 0
        CHECK (exact_rrf_contribution >= 0),
    ADD COLUMN lexical_rrf_contribution numeric(20, 12) NOT NULL DEFAULT 0
        CHECK (lexical_rrf_contribution >= 0),
    ADD COLUMN vector_rrf_contribution numeric(20, 12) NOT NULL DEFAULT 0
        CHECK (vector_rrf_contribution >= 0),
    ADD COLUMN fused_score numeric(20, 12) CHECK (
        fused_score IS NULL OR fused_score >= 0
    ),
    ADD COLUMN embedding_profile_sha256 character(64) CHECK (
        embedding_profile_sha256 IS NULL
        OR embedding_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN embedding_projection_profile_sha256 character(64) CHECK (
        embedding_projection_profile_sha256 IS NULL
        OR embedding_projection_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN embedding_input_sha256 character(64) CHECK (
        embedding_input_sha256 IS NULL
        OR embedding_input_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN embedding_vector_sha256 character(64) CHECK (
        embedding_vector_sha256 IS NULL
        OR embedding_vector_sha256 ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE memory.retrieval_manifest_items
    DROP CONSTRAINT retrieval_manifest_items_check,
    ADD CONSTRAINT retrieval_manifest_items_has_channel CHECK (
        exact_identity_rank IS NOT NULL
        OR exact_rank IS NOT NULL
        OR lexical_rank IS NOT NULL
        OR vector_rank IS NOT NULL
    ),
    ADD CONSTRAINT retrieval_manifest_items_vector_pair CHECK (
        (vector_rank IS NULL AND vector_distance IS NULL AND vector_similarity IS NULL)
        OR (
            vector_rank IS NOT NULL
            AND vector_distance IS NOT NULL
            AND vector_similarity IS NOT NULL
        )
    ),
    ADD CONSTRAINT retrieval_manifest_items_rrf_pairs CHECK (
        (exact_rank IS NOT NULL OR exact_rrf_contribution = 0)
        AND (lexical_rank IS NOT NULL OR lexical_rrf_contribution = 0)
        AND (vector_rank IS NOT NULL OR vector_rrf_contribution = 0)
    ),
    ADD CONSTRAINT retrieval_manifest_items_embedding_lineage_complete CHECK (
        (
            embedding_profile_sha256 IS NULL
            AND embedding_projection_profile_sha256 IS NULL
            AND embedding_input_sha256 IS NULL
            AND embedding_vector_sha256 IS NULL
        )
        OR (
            embedding_profile_sha256 IS NOT NULL
            AND embedding_projection_profile_sha256 IS NOT NULL
            AND embedding_input_sha256 IS NOT NULL
            AND embedding_vector_sha256 IS NOT NULL
        )
    );

CREATE FUNCTION memory.validate_retrieval_manifest_item_channels()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    receipt_embedding_profile_sha256 character(64);
    receipt_projection_profile_sha256 character(64);
    receipt_query_vector_sha256 character(64);
    receipt_mode text;
    receipt_rrf_k integer;
    receipt_score_scale integer;
BEGIN
    SELECT
        receipt.embedding_profile_sha256,
        receipt.embedding_projection_profile_sha256,
        receipt.query_vector_sha256,
        policy.retrieval_mode,
        (policy.policy_document #>> '{fusion,k}')::integer,
        (policy.policy_document ->> 'score_scale')::integer
    INTO
        receipt_embedding_profile_sha256,
        receipt_projection_profile_sha256,
        receipt_query_vector_sha256,
        receipt_mode,
        receipt_rrf_k,
        receipt_score_scale
    FROM memory.retrieval_receipts AS receipt
    JOIN memory.retrieval_policies AS policy
      ON policy.policy_id = receipt.policy_id
     AND policy.policy_version = receipt.policy_version
     AND policy.policy_sha256 = receipt.policy_sha256
    WHERE receipt.tenant_id = NEW.tenant_id
      AND receipt.subject_id = NEW.subject_id
      AND receipt.retrieval_id = NEW.retrieval_id
      AND receipt.principal_id = NEW.principal_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'retrieval receipt is unavailable'
            USING ERRCODE = '23503';
    END IF;

    IF receipt_mode = 'lexical' AND (
        NEW.exact_rank IS NOT NULL
        OR NEW.vector_rank IS NOT NULL
        OR NEW.vector_distance IS NOT NULL
        OR NEW.vector_similarity IS NOT NULL
        OR NEW.exact_rrf_contribution <> 0
        OR NEW.lexical_rrf_contribution <> 0
        OR NEW.vector_rrf_contribution <> 0
        OR NEW.fused_score IS NOT NULL
        OR NEW.embedding_profile_sha256 IS NOT NULL
        OR NEW.embedding_projection_profile_sha256 IS NOT NULL
        OR NEW.embedding_input_sha256 IS NOT NULL
        OR NEW.embedding_vector_sha256 IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'lexical manifest cannot contain fusion lineage'
            USING ERRCODE = '23514';
    ELSIF receipt_mode = 'hybrid' AND (
        NEW.exact_rank IS NULL
            AND NEW.lexical_rank IS NULL
            AND NEW.vector_rank IS NULL
        OR (NEW.exact_rank IS NULL) <> (NEW.exact_rrf_contribution = 0)
        OR (NEW.lexical_rank IS NULL) <> (NEW.lexical_rrf_contribution = 0)
        OR (NEW.vector_rank IS NULL) <> (NEW.vector_rrf_contribution = 0)
        OR NEW.fused_score IS NULL
        OR NEW.final_score <> NEW.fused_score
        OR NEW.embedding_profile_sha256
            IS DISTINCT FROM receipt_embedding_profile_sha256
        OR NEW.embedding_projection_profile_sha256
            IS DISTINCT FROM receipt_projection_profile_sha256
        OR NEW.embedding_input_sha256 IS NULL
        OR NEW.embedding_vector_sha256 IS NULL
        OR receipt_query_vector_sha256 IS NULL
        OR NEW.exact_rrf_contribution IS DISTINCT FROM CASE
            WHEN NEW.exact_rank IS NULL THEN 0::numeric
            ELSE round(
                1::numeric / (receipt_rrf_k + NEW.exact_rank),
                receipt_score_scale
            )
        END
        OR NEW.lexical_rrf_contribution IS DISTINCT FROM CASE
            WHEN NEW.lexical_rank IS NULL THEN 0::numeric
            ELSE round(
                1::numeric / (receipt_rrf_k + NEW.lexical_rank),
                receipt_score_scale
            )
        END
        OR NEW.vector_rrf_contribution IS DISTINCT FROM CASE
            WHEN NEW.vector_rank IS NULL THEN 0::numeric
            ELSE round(
                1::numeric / (receipt_rrf_k + NEW.vector_rank),
                receipt_score_scale
            )
        END
        OR NEW.fused_score IS DISTINCT FROM (
            NEW.exact_rrf_contribution
            + NEW.lexical_rrf_contribution
            + NEW.vector_rrf_contribution
        )
    ) THEN
        RAISE EXCEPTION 'hybrid manifest fusion lineage is invalid'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER retrieval_manifest_items_validate_channels
BEFORE INSERT ON memory.retrieval_manifest_items
FOR EACH ROW EXECUTE FUNCTION memory.validate_retrieval_manifest_item_channels();

-- Append the fusion explanation fields without changing the identity or order
-- of the existing lexical columns consumed by pre-migration receipts.
CREATE OR REPLACE VIEW memory.authorized_retrieval_manifest
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
    item.schema_version,
    item.exact_rank,
    item.vector_rank,
    item.vector_distance,
    item.vector_similarity,
    item.exact_rrf_contribution,
    item.lexical_rrf_contribution,
    item.vector_rrf_contribution,
    item.fused_score,
    item.embedding_profile_sha256,
    item.embedding_projection_profile_sha256,
    item.embedding_input_sha256,
    item.embedding_vector_sha256
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
