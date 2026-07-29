-- Immutable recency profiles are canonical policy artifacts. Retrieval code
-- consumes their registered identity and digest; PostgreSQL never evaluates
-- the continuous decay curve with host-dependent floating-point math.
CREATE TABLE memory.recency_profiles (
    profile_id text NOT NULL CHECK (
        btrim(profile_id) <> '' AND length(profile_id) <= 255
    ),
    profile_version text NOT NULL CHECK (
        btrim(profile_version) <> '' AND length(profile_version) <= 255
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
    UNIQUE (profile_id, profile_version, profile_sha256)
);

CREATE FUNCTION memory.validate_recency_profile_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    calculated_constants_sha256 text;
    active_constants_are_numbers boolean;
BEGIN
    IF NEW.profile_id = 'active-case-30d-v1'
        AND jsonb_typeof(NEW.profile_document -> 'q63_constants') = 'array'
    THEN
        SELECT encode(
            sha256(
                convert_to(
                    'q63-exp2-v1' || E'\n'
                    || string_agg(
                        constant.ordinality::text || '=' || constant.value,
                        E'\n'
                        ORDER BY constant.ordinality
                    )
                    || E'\n',
                    'UTF8'
                )
            ),
            'hex'
        )
        INTO calculated_constants_sha256
        FROM jsonb_array_elements_text(
            NEW.profile_document -> 'q63_constants'
        ) WITH ORDINALITY AS constant(value, ordinality);

        SELECT bool_and(jsonb_typeof(constant.value) = 'number')
        INTO active_constants_are_numbers
        FROM jsonb_array_elements(
            NEW.profile_document -> 'q63_constants'
        ) AS constant(value);
    END IF;

    IF NEW.profile_sha256 <> encode(
        sha256(convert_to(NEW.profile_document::text, 'UTF8')),
        'hex'
    )
        OR NEW.profile_document ->> 'profile_id'
            IS DISTINCT FROM NEW.profile_id
        OR NEW.profile_document ->> 'profile_version'
            IS DISTINCT FROM NEW.profile_version
        OR (NEW.profile_document ->> 'schema_version')::integer
            IS DISTINCT FROM NEW.schema_version
        OR (
            NEW.profile_id = 'stable-v1'
            AND NEW.profile_document IS DISTINCT FROM jsonb_build_object(
                'profile_id', 'stable-v1',
                'profile_version', '1',
                'kind', 'stable',
                'factor_units', 1000000000000::bigint,
                'score_scale', 12,
                'time_unit', 'microsecond',
                'rounding', 'half-even',
                'schema_version', 1
            )
        )
        OR (
            NEW.profile_id = 'active-case-30d-v1'
            AND (
                CASE
                    WHEN jsonb_typeof(
                        NEW.profile_document -> 'q63_constants'
                    ) = 'array'
                    THEN jsonb_array_length(
                        NEW.profile_document -> 'q63_constants'
                    ) <> 63
                        OR active_constants_are_numbers IS DISTINCT FROM true
                        OR NEW.profile_document ->> 'q63_constants_sha256'
                            IS DISTINCT FROM calculated_constants_sha256
                        OR calculated_constants_sha256 IS DISTINCT FROM
                            '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa'
                    ELSE true
                END
                OR NEW.profile_document - 'q63_constants'
                    IS DISTINCT FROM jsonb_build_object(
                        'profile_id', 'active-case-30d-v1',
                        'profile_version', '1',
                        'kind', 'continuous-half-life',
                        'anchor_source', 'revision-observed-at',
                        'negative_age', 'clamp-to-zero',
                        'time_unit', 'microsecond',
                        'half_life_us', 2592000000000::bigint,
                        'floor_q63_units', 1152921504606846976::bigint,
                        'q63_scale_units', 9223372036854775808::numeric,
                        'q63_algorithm', 'exp2-negative-binary-powers-v1',
                        'q63_constants_generator', jsonb_build_object(
                            'script', 'scripts/generate-q63-exp2.py',
                            'mpfr_version', '4.2.2',
                            'gmp_version', '6.3.0',
                            'working_precision_bits', 256,
                            'rounding', 'MPFR_RNDN'
                        ),
                        'q63_constants_sha256',
                            '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa',
                        'q63_max_absolute_error_units', 64,
                        'score_scale', 12,
                        'rounding', 'half-even',
                        'schema_version', 1
                    )
            )
        )
    THEN
        RAISE EXCEPTION 'recency profile registration is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'recency_profile_registration_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER recency_profiles_validate_registration
BEFORE INSERT ON memory.recency_profiles
FOR EACH ROW EXECUTE FUNCTION memory.validate_recency_profile_registration();

CREATE TRIGGER recency_profiles_reject_mutation
BEFORE UPDATE OR DELETE ON memory.recency_profiles
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

WITH profiles(profile_id, profile_version, profile_document) AS (
    VALUES
    (
        'stable-v1',
        '1',
        jsonb_build_object(
            'profile_id', 'stable-v1',
            'profile_version', '1',
            'kind', 'stable',
            'factor_units', 1000000000000,
            'score_scale', 12,
            'time_unit', 'microsecond',
            'rounding', 'half-even',
            'schema_version', 1
        )
    ),
    (
        'active-case-30d-v1',
        '1',
        jsonb_build_object(
            'profile_id', 'active-case-30d-v1',
            'profile_version', '1',
            'kind', 'continuous-half-life',
            'anchor_source', 'revision-observed-at',
            'negative_age', 'clamp-to-zero',
            'time_unit', 'microsecond',
            'half_life_us', 2592000000000,
            'floor_q63_units', 1152921504606846976,
            'q63_scale_units', 9223372036854775808,
            'q63_algorithm', 'exp2-negative-binary-powers-v1',
            'q63_constants_generator', jsonb_build_object(
                'script', 'scripts/generate-q63-exp2.py',
                'mpfr_version', '4.2.2',
                'gmp_version', '6.3.0',
                'working_precision_bits', 256,
                'rounding', 'MPFR_RNDN'
            ),
            'q63_constants_sha256',
                '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa',
            'q63_max_absolute_error_units', 64,
            'q63_constants', jsonb_build_array(
                6521908912666391106,
                7755900482342532474,
                8457869449776733335,
                8832331321595618838,
                9025734193507008925,
                9124017994966720698,
                9173560510430823462,
                9198432556164277331,
                9210893855724328809,
                9217130834664616070,
                9220250907674776491,
                9221811340221203999,
                9222591655524303666,
                9222981837935769002,
                9223176935331786073,
                9223274485577403901,
                9223323261087119913,
                9223347648938705290,
                9223359842888679895,
                9223365939869712687,
                9223368988361740456,
                9223370512608132184,
                9223371274731422509,
                9223371655793091287,
                9223371846323931579,
                9223371941589353202,
                9223371989222064382,
                9223372013038420064,
                9223372024946597928,
                9223372030900686866,
                9223372033877731337,
                9223372035366253572,
                9223372036110514690,
                9223372036482645249,
                9223372036668710529,
                9223372036761743168,
                9223372036808259488,
                9223372036831517648,
                9223372036843146728,
                9223372036848961268,
                9223372036851868538,
                9223372036853322173,
                9223372036854048991,
                9223372036854412399,
                9223372036854594104,
                9223372036854684956,
                9223372036854730382,
                9223372036854753095,
                9223372036854764451,
                9223372036854770130,
                9223372036854772969,
                9223372036854774388,
                9223372036854775098,
                9223372036854775453,
                9223372036854775631,
                9223372036854775719,
                9223372036854775764,
                9223372036854775786,
                9223372036854775797,
                9223372036854775802,
                9223372036854775805,
                9223372036854775807,
                9223372036854775807
            ),
            'score_scale', 12,
            'rounding', 'half-even',
            'schema_version', 1
        )
    )
)
INSERT INTO memory.recency_profiles (
    profile_id,
    profile_version,
    profile_document,
    profile_sha256,
    schema_version
)
SELECT
    profile_id,
    profile_version,
    profile_document,
    encode(sha256(convert_to(profile_document::text, 'UTF8')), 'hex'),
    1
FROM profiles;

-- A write policy is the trusted assignment boundary. The public fact command
-- names a registered write policy; it never supplies recency or importance as
-- retrieval controls.
CREATE TABLE memory.fact_retrieval_metadata_policies (
    policy_id text NOT NULL CHECK (
        btrim(policy_id) <> '' AND length(policy_id) <= 255
    ),
    policy_version text NOT NULL CHECK (
        btrim(policy_version) <> '' AND length(policy_version) <= 255
    ),
    recency_profile_id text NOT NULL,
    recency_profile_version text NOT NULL,
    recency_profile_sha256 character(64) NOT NULL CHECK (
        recency_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    recency_anchor_source text NOT NULL CHECK (
        recency_anchor_source = 'revision-observed-at'
    ),
    importance numeric(5, 4) NOT NULL CHECK (importance BETWEEN 0 AND 1),
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
    UNIQUE (policy_id, policy_version, policy_sha256),
    FOREIGN KEY (
        recency_profile_id,
        recency_profile_version,
        recency_profile_sha256
    ) REFERENCES memory.recency_profiles (
        profile_id,
        profile_version,
        profile_sha256
    )
);

CREATE FUNCTION memory.validate_fact_retrieval_metadata_policy_registration()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    IF NEW.policy_sha256 <> encode(
        sha256(convert_to(NEW.policy_document::text, 'UTF8')),
        'hex'
    )
        OR NEW.policy_document #>> '{write_policy,id}'
            IS DISTINCT FROM NEW.policy_id
        OR NEW.policy_document #>> '{write_policy,version}'
            IS DISTINCT FROM NEW.policy_version
        OR NEW.policy_document #>> '{recency_profile,id}'
            IS DISTINCT FROM NEW.recency_profile_id
        OR NEW.policy_document #>> '{recency_profile,version}'
            IS DISTINCT FROM NEW.recency_profile_version
        OR NEW.policy_document #>> '{recency_profile,digest}'
            IS DISTINCT FROM NEW.recency_profile_sha256
        OR NEW.policy_document ->> 'recency_anchor_source'
            IS DISTINCT FROM NEW.recency_anchor_source
        OR (NEW.policy_document ->> 'importance')::numeric
            IS DISTINCT FROM NEW.importance
        OR (NEW.policy_document ->> 'schema_version')::integer
            IS DISTINCT FROM NEW.schema_version
    THEN
        RAISE EXCEPTION 'fact retrieval metadata policy registration is inconsistent'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'fact_retrieval_metadata_policy_registration_consistent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER fact_retrieval_metadata_policies_validate_registration
BEFORE INSERT ON memory.fact_retrieval_metadata_policies
FOR EACH ROW
EXECUTE FUNCTION memory.validate_fact_retrieval_metadata_policy_registration();

CREATE TRIGGER fact_retrieval_metadata_policies_reject_mutation
BEFORE UPDATE OR DELETE ON memory.fact_retrieval_metadata_policies
FOR EACH ROW EXECUTE FUNCTION memory.reject_fact_history_mutation();

-- Close the live-write gap while the migration registers every policy already
-- present in canonical revisions and backfills its governance lineage.
LOCK TABLE memory.fact_revisions IN SHARE ROW EXCLUSIVE MODE;

WITH write_policies AS (
    SELECT 'direct-evidence'::text AS policy_id, '1'::text AS policy_version
    UNION
    SELECT DISTINCT write_policy_id, write_policy_version
    FROM memory.fact_revisions
), stable AS (
    SELECT profile_id, profile_version, profile_sha256
    FROM memory.recency_profiles
    WHERE profile_id = 'stable-v1' AND profile_version = '1'
), assignments AS (
    SELECT
        write_policy.policy_id,
        write_policy.policy_version,
        stable.profile_id,
        stable.profile_version,
        stable.profile_sha256,
        jsonb_build_object(
            'write_policy', jsonb_build_object(
                'id', write_policy.policy_id,
                'version', write_policy.policy_version
            ),
            'recency_profile', jsonb_build_object(
                'id', stable.profile_id,
                'version', stable.profile_version,
                'digest', stable.profile_sha256
            ),
            'recency_anchor_source', 'revision-observed-at',
            'importance', 0.5000,
            'schema_version', 1
        ) AS policy_document
    FROM write_policies AS write_policy
    CROSS JOIN stable
)
INSERT INTO memory.fact_retrieval_metadata_policies (
    policy_id,
    policy_version,
    recency_profile_id,
    recency_profile_version,
    recency_profile_sha256,
    recency_anchor_source,
    importance,
    policy_document,
    policy_sha256,
    schema_version
)
SELECT
    policy_id,
    policy_version,
    profile_id,
    profile_version,
    profile_sha256,
    'revision-observed-at',
    0.5000,
    policy_document,
    encode(sha256(convert_to(policy_document::text, 'UTF8')), 'hex'),
    1
FROM assignments;

DROP TRIGGER fact_revision_governance_restrict_mutation
    ON memory.fact_revision_governance;

ALTER TABLE memory.fact_revision_governance
    ADD COLUMN recency_profile_version text,
    ADD COLUMN recency_profile_sha256 character(64) CHECK (
        recency_profile_sha256 IS NULL
        OR recency_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN recency_anchor_at timestamptz CHECK (
        recency_anchor_at IS NULL OR isfinite(recency_anchor_at)
    ),
    ADD COLUMN metadata_policy_id text,
    ADD COLUMN metadata_policy_version text,
    ADD COLUMN metadata_policy_sha256 character(64) CHECK (
        metadata_policy_sha256 IS NULL
        OR metadata_policy_sha256 ~ '^[0-9a-f]{64}$'
    );

UPDATE memory.fact_revision_governance AS governance
SET
    recency_profile_version = assignment.recency_profile_version,
    recency_profile_sha256 = assignment.recency_profile_sha256,
    recency_anchor_at = revision.observed_at,
    metadata_policy_id = assignment.policy_id,
    metadata_policy_version = assignment.policy_version,
    metadata_policy_sha256 = assignment.policy_sha256
FROM memory.fact_revisions AS revision
JOIN memory.fact_retrieval_metadata_policies AS assignment
  ON assignment.policy_id = revision.write_policy_id
 AND assignment.policy_version = revision.write_policy_version
WHERE governance.tenant_id = revision.tenant_id
  AND governance.subject_id = revision.subject_id
  AND governance.case_id = revision.case_id
  AND governance.fact_id = revision.fact_id
  AND governance.revision_id = revision.revision_id
  AND governance.recency_profile_id = assignment.recency_profile_id
  AND governance.importance = assignment.importance;

ALTER TABLE memory.fact_revision_governance
    ALTER COLUMN recency_profile_version SET NOT NULL,
    ALTER COLUMN recency_profile_sha256 SET NOT NULL,
    ALTER COLUMN recency_anchor_at SET NOT NULL,
    ALTER COLUMN metadata_policy_id SET NOT NULL,
    ALTER COLUMN metadata_policy_version SET NOT NULL,
    ALTER COLUMN metadata_policy_sha256 SET NOT NULL,
    ADD FOREIGN KEY (
        recency_profile_id,
        recency_profile_version,
        recency_profile_sha256
    ) REFERENCES memory.recency_profiles (
        profile_id,
        profile_version,
        profile_sha256
    ),
    ADD FOREIGN KEY (
        metadata_policy_id,
        metadata_policy_version,
        metadata_policy_sha256
    ) REFERENCES memory.fact_retrieval_metadata_policies (
        policy_id,
        policy_version,
        policy_sha256
    );

CREATE OR REPLACE FUNCTION memory.populate_fact_revision_retrieval_metadata()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    retention_duration interval;
    stored_namespace text;
    stored_key text;
    projection_digest character(64);
    assigned_recency_profile_id text;
    assigned_recency_profile_version text;
    assigned_recency_profile_sha256 character(64);
    assigned_importance numeric(5, 4);
    assigned_policy_sha256 character(64);
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

    SELECT
        assignment.recency_profile_id,
        assignment.recency_profile_version,
        assignment.recency_profile_sha256,
        assignment.importance,
        assignment.policy_sha256
    INTO
        assigned_recency_profile_id,
        assigned_recency_profile_version,
        assigned_recency_profile_sha256,
        assigned_importance,
        assigned_policy_sha256
    FROM memory.fact_retrieval_metadata_policies AS assignment
    WHERE assignment.policy_id = NEW.write_policy_id
      AND assignment.policy_version = NEW.write_policy_version;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'fact write policy has no retrieval metadata assignment'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'fact_retrieval_metadata_policy_known';
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
        recency_profile_version,
        recency_profile_sha256,
        recency_anchor_at,
        importance,
        metadata_policy_id,
        metadata_policy_version,
        metadata_policy_sha256,
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
        assigned_recency_profile_id,
        assigned_recency_profile_version,
        assigned_recency_profile_sha256,
        NEW.observed_at,
        assigned_importance,
        NEW.write_policy_id,
        NEW.write_policy_version,
        assigned_policy_sha256,
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

CREATE OR REPLACE FUNCTION memory.restrict_fact_revision_governance_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
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
        OR OLD.recency_profile_version <> NEW.recency_profile_version
        OR OLD.recency_profile_sha256 <> NEW.recency_profile_sha256
        OR OLD.recency_anchor_at <> NEW.recency_anchor_at
        OR OLD.importance <> NEW.importance
        OR OLD.metadata_policy_id <> NEW.metadata_policy_id
        OR OLD.metadata_policy_version <> NEW.metadata_policy_version
        OR OLD.metadata_policy_sha256 <> NEW.metadata_policy_sha256
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

ALTER TABLE memory.recency_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.recency_profiles FORCE ROW LEVEL SECURITY;
CREATE POLICY recency_profiles_select
    ON memory.recency_profiles FOR SELECT USING (true);

ALTER TABLE memory.fact_retrieval_metadata_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_retrieval_metadata_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY fact_retrieval_metadata_policies_select
    ON memory.fact_retrieval_metadata_policies FOR SELECT USING (true);

-- Preserve every existing policy document and digest. The discriminator is a
-- new additive column so old policies remain channel-only without rewriting
-- their immutable JSON artifacts.
ALTER TABLE memory.retrieval_policies
    ADD COLUMN scoring_mode text NOT NULL DEFAULT 'channel-only'
        CHECK (scoring_mode IN ('channel-only', 'temporal-v1'));

CREATE OR REPLACE FUNCTION memory.validate_retrieval_policy_registration()
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
        OR (
            NEW.scoring_mode = 'temporal-v1'
            AND (
                NEW.retrieval_mode IS DISTINCT FROM 'hybrid'
                OR NEW.policy_id
                    IS DISTINCT FROM 'retrieval-hybrid-temporal-v1'
                OR NEW.policy_document ->> 'rounding'
                    IS DISTINCT FROM 'half-even'
                OR NEW.policy_document #>> '{arithmetic,id}'
                    IS DISTINCT FROM 'score-units-q63-v1'
                OR (NEW.policy_document #>> '{arithmetic,score_scale}')::integer
                    IS DISTINCT FROM 12
                OR NEW.policy_document #>> '{arithmetic,rounding}'
                    IS DISTINCT FROM 'half-even'
                OR NEW.policy_document #>> '{arithmetic,overflow}'
                    IS DISTINCT FROM 'reject'
                OR NEW.policy_document #> '{arithmetic,operation_order}'
                    IS DISTINCT FROM jsonb_build_array(
                        'rrf-channel-half-even',
                        'fused-exact-sum',
                        'recency-half-even',
                        'confidence-half-even',
                        'importance-half-even',
                        'exact-identity-bonus'
                    )
                OR (NEW.policy_document #>> '{fusion,k}')::integer
                    IS DISTINCT FROM 60
                OR (NEW.policy_document #>> '{fusion,weights,exact}')::integer
                    IS DISTINCT FROM 1
                OR (NEW.policy_document #>> '{fusion,weights,lexical}')::integer
                    IS DISTINCT FROM 1
                OR (NEW.policy_document #>> '{fusion,weights,vector}')::integer
                    IS DISTINCT FROM 1
                OR (NEW.policy_document ->> 'score_scale')::integer
                    IS DISTINCT FROM 12
                OR COALESCE(
                    (NEW.policy_document #>> '{candidate_limits,exact}')::integer
                        NOT BETWEEN 1 AND 50,
                    true
                )
                OR COALESCE(
                    (NEW.policy_document #>> '{candidate_limits,lexical}')::integer
                        NOT BETWEEN 1 AND 50,
                    true
                )
                OR COALESCE(
                    (NEW.policy_document #>> '{candidate_limits,vector}')::integer
                        NOT BETWEEN 1 AND 50,
                    true
                )
                OR COALESCE(
                    (NEW.policy_document ->> 'manifest_limit')::integer
                        NOT BETWEEN 1 AND 50,
                    true
                )
                OR (NEW.policy_document ->> 'fts_rank_normalization')::integer
                    IS DISTINCT FROM 32
                OR (NEW.policy_document ->> 'exact_identity_precedence')::boolean
                    IS DISTINCT FROM true
                OR NEW.policy_document -> 'tie_break'
                    IS DISTINCT FROM jsonb_build_array(
                        'exact_identity_rank_asc_nulls_last',
                        'final_score_units_desc',
                        'exact_rank_asc_nulls_last',
                        'lexical_rank_asc_nulls_last',
                        'vector_rank_asc_nulls_last',
                        'case_id_asc',
                        'fact_id_asc',
                        'revision_id_asc'
                    )
                OR NEW.policy_document #> '{channel_tie_breaks,exact}'
                    IS DISTINCT FROM jsonb_build_array(
                        'exact_identity_rank_asc',
                        'case_id_asc',
                        'fact_id_asc',
                        'revision_id_asc'
                    )
                OR NEW.policy_document #> '{channel_tie_breaks,lexical}'
                    IS DISTINCT FROM jsonb_build_array(
                        'lexical_score_desc',
                        'case_id_asc',
                        'fact_id_asc',
                        'revision_id_asc'
                    )
                OR NEW.policy_document #> '{channel_tie_breaks,vector}'
                    IS DISTINCT FROM jsonb_build_array(
                        'vector_distance_asc',
                        'case_id_asc',
                        'fact_id_asc',
                        'revision_id_asc'
                    )
                OR NEW.policy_document #>> '{temporal,axis}'
                    IS DISTINCT FROM 'request.valid_at'
                OR NEW.policy_document #>> '{temporal,anchor}'
                    IS DISTINCT FROM
                        'fact_revision_governance.recency_anchor_at'
                OR NEW.policy_document #>> '{temporal,age_unit}'
                    IS DISTINCT FROM 'microsecond'
                OR NEW.policy_document #>> '{temporal,negative_age}'
                    IS DISTINCT FROM 'clamp_zero'
                OR NEW.policy_document #>>
                    '{temporal,profile_lineage,stable-v1,version}'
                    IS DISTINCT FROM '1'
                OR NEW.policy_document #>>
                    '{temporal,profile_lineage,stable-v1,digest}'
                    IS DISTINCT FROM (
                        SELECT profile_sha256
                        FROM memory.recency_profiles
                        WHERE profile_id = 'stable-v1'
                          AND profile_version = '1'
                    )
                OR NEW.policy_document #>>
                    '{temporal,profile_lineage,active-case-30d-v1,version}'
                    IS DISTINCT FROM '1'
                OR NEW.policy_document #>>
                    '{temporal,profile_lineage,active-case-30d-v1,digest}'
                    IS DISTINCT FROM (
                        SELECT profile_sha256
                        FROM memory.recency_profiles
                        WHERE profile_id = 'active-case-30d-v1'
                          AND profile_version = '1'
                    )
                OR NEW.policy_document #>>
                    '{temporal,profiles,stable-v1,kind}'
                    IS DISTINCT FROM 'constant'
                OR NEW.policy_document #>>
                    '{temporal,profiles,stable-v1,factor_units}'
                    IS DISTINCT FROM '1000000000000'
                OR NEW.policy_document #>>
                    '{temporal,profiles,active-case-30d-v1,kind}'
                    IS DISTINCT FROM 'continuous-half-life'
                OR NEW.policy_document #>>
                    '{temporal,profiles,active-case-30d-v1,half_life_us}'
                    IS DISTINCT FROM '2592000000000'
                OR NEW.policy_document #>>
                    '{temporal,profiles,active-case-30d-v1,floor_units}'
                    IS DISTINCT FROM '125000000000'
                OR NEW.policy_document #>>
                    '{temporal,profiles,active-case-30d-v1,arithmetic}'
                    IS DISTINCT FROM 'q63-exp2-v1'
                OR NEW.policy_document #>>
                    '{temporal,profiles,active-case-30d-v1,constants_sha256}'
                    IS DISTINCT FROM
                        '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa'
                OR NEW.policy_document #>> '{quality_factors,confidence,source}'
                    IS DISTINCT FROM 'fact_revisions.confidence'
                OR NEW.policy_document #>> '{quality_factors,confidence,formula}'
                    IS DISTINCT FROM 'identity'
                OR NEW.policy_document #>>
                    '{quality_factors,confidence,minimum_units}'
                    IS DISTINCT FROM '0'
                OR NEW.policy_document #>>
                    '{quality_factors,confidence,maximum_units}'
                    IS DISTINCT FROM '1000000000000'
                OR NEW.policy_document #>> '{quality_factors,importance,source}'
                    IS DISTINCT FROM 'fact_revision_governance.importance'
                OR NEW.policy_document #>> '{quality_factors,importance,formula}'
                    IS DISTINCT FROM 'offset-plus-value'
                OR NEW.policy_document #>>
                    '{quality_factors,importance,offset_units}'
                    IS DISTINCT FROM '500000000000'
                OR NEW.policy_document #>>
                    '{quality_factors,importance,minimum_units}'
                    IS DISTINCT FROM '500000000000'
                OR NEW.policy_document #>>
                    '{quality_factors,importance,maximum_units}'
                    IS DISTINCT FROM '1500000000000'
                OR NEW.policy_document #>>
                    '{exact_identity_bonus_units,namespace_key}'
                    IS DISTINCT FROM '16393442623'
                OR NEW.policy_document #>> '{exact_identity_bonus_units,key}'
                    IS DISTINCT FROM '8196721311'
                OR NEW.policy_document #>> '{exact_identity_bonus_units,none}'
                    IS DISTINCT FROM '0'
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

WITH recency AS (
    SELECT jsonb_object_agg(
        profile_id,
        jsonb_build_object(
            'version', profile_version,
            'digest', profile_sha256
        )
        ORDER BY profile_id
    ) AS lineage
    FROM memory.recency_profiles
    WHERE (profile_id, profile_version) IN (
        ('stable-v1', '1'),
        ('active-case-30d-v1', '1')
    )
), temporal_policy AS (
    SELECT
        base.embedding_profile_id,
        base.embedding_profile_version,
        base.embedding_profile_sha256,
        base.embedding_projection_profile_id,
        base.embedding_projection_profile_version,
        base.embedding_projection_profile_sha256,
        base.policy_document || jsonb_build_object(
            'rounding', 'half-even',
            'tie_break', jsonb_build_array(
                'exact_identity_rank_asc_nulls_last',
                'final_score_units_desc',
                'exact_rank_asc_nulls_last',
                'lexical_rank_asc_nulls_last',
                'vector_rank_asc_nulls_last',
                'case_id_asc',
                'fact_id_asc',
                'revision_id_asc'
            ),
            'arithmetic', jsonb_build_object(
                'id', 'score-units-q63-v1',
                'score_scale', 12,
                'rounding', 'half-even',
                'overflow', 'reject',
                'operation_order', jsonb_build_array(
                    'rrf-channel-half-even',
                    'fused-exact-sum',
                    'recency-half-even',
                    'confidence-half-even',
                    'importance-half-even',
                    'exact-identity-bonus'
                )
            ),
            'temporal', jsonb_build_object(
                'axis', 'request.valid_at',
                'anchor', 'fact_revision_governance.recency_anchor_at',
                'age_unit', 'microsecond',
                'negative_age', 'clamp_zero',
                'profile_lineage', recency.lineage,
                'profiles', jsonb_build_object(
                    'stable-v1', jsonb_build_object(
                        'kind', 'constant',
                        'factor_units', '1000000000000'
                    ),
                    'active-case-30d-v1', jsonb_build_object(
                        'kind', 'continuous-half-life',
                        'half_life_us', '2592000000000',
                        'floor_units', '125000000000',
                        'arithmetic', 'q63-exp2-v1',
                        'constants_sha256',
                            '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa'
                    )
                )
            ),
            'quality_factors', jsonb_build_object(
                'confidence', jsonb_build_object(
                    'source', 'fact_revisions.confidence',
                    'formula', 'identity',
                    'minimum_units', '0',
                    'maximum_units', '1000000000000'
                ),
                'importance', jsonb_build_object(
                    'source', 'fact_revision_governance.importance',
                    'formula', 'offset-plus-value',
                    'offset_units', '500000000000',
                    'minimum_units', '500000000000',
                    'maximum_units', '1500000000000'
                )
            ),
            'exact_identity_bonus_units', jsonb_build_object(
                'namespace_key', '16393442623',
                'key', '8196721311',
                'none', '0'
            )
        ) AS policy_document
    FROM memory.retrieval_policies AS base
    CROSS JOIN recency
    WHERE base.policy_id = 'retrieval-hybrid-v1'
      AND base.policy_version = '1'
      AND base.retrieval_mode = 'hybrid'
      AND base.scoring_mode = 'channel-only'
)
INSERT INTO memory.retrieval_policies (
    policy_id,
    policy_version,
    policy_document,
    policy_sha256,
    schema_version,
    retrieval_mode,
    embedding_profile_id,
    embedding_profile_version,
    embedding_profile_sha256,
    embedding_projection_profile_id,
    embedding_projection_profile_version,
    embedding_projection_profile_sha256,
    scoring_mode
)
SELECT
    'retrieval-hybrid-temporal-v1',
    '1',
    policy_document,
    encode(sha256(convert_to(policy_document::text, 'UTF8')), 'hex'),
    1,
    'hybrid',
    embedding_profile_id,
    embedding_profile_version,
    embedding_profile_sha256,
    embedding_projection_profile_id,
    embedding_projection_profile_version,
    embedding_projection_profile_sha256,
    'temporal-v1'
FROM temporal_policy;

-- Temporal explanations are additive and nullable so pre-0008 item JSON and
-- digests remain byte-identical after upgrade. A version-2 temporal item must
-- populate the complete lineage and score breakdown as one unit.
ALTER TABLE memory.retrieval_manifest_items
    ADD COLUMN recency_profile_id text,
    ADD COLUMN recency_profile_version text,
    ADD COLUMN recency_profile_sha256 character(64) CHECK (
        recency_profile_sha256 IS NULL
        OR recency_profile_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN recency_anchor_at timestamptz CHECK (
        recency_anchor_at IS NULL OR isfinite(recency_anchor_at)
    ),
    ADD COLUMN recency_age_us numeric(30, 0) CHECK (
        recency_age_us IS NULL OR recency_age_us >= 0
    ),
    ADD COLUMN recency_factor numeric(20, 12) CHECK (
        recency_factor IS NULL OR recency_factor BETWEEN 0 AND 1
    ),
    ADD COLUMN confidence_factor numeric(20, 12) CHECK (
        confidence_factor IS NULL OR confidence_factor BETWEEN 0 AND 1
    ),
    ADD COLUMN importance_factor numeric(20, 12) CHECK (
        importance_factor IS NULL OR importance_factor BETWEEN 0.5 AND 1.5
    ),
    ADD COLUMN temporal_adjustment numeric(20, 12),
    ADD COLUMN confidence_adjustment numeric(20, 12),
    ADD COLUMN importance_adjustment numeric(20, 12),
    ADD COLUMN exact_identity_bonus numeric(20, 12) CHECK (
        exact_identity_bonus IS NULL OR exact_identity_bonus >= 0
    ),
    ADD CONSTRAINT retrieval_manifest_items_temporal_lineage_complete CHECK (
        (
            recency_profile_id IS NULL
            AND recency_profile_version IS NULL
            AND recency_profile_sha256 IS NULL
            AND recency_anchor_at IS NULL
            AND recency_age_us IS NULL
            AND recency_factor IS NULL
            AND confidence_factor IS NULL
            AND importance_factor IS NULL
            AND temporal_adjustment IS NULL
            AND confidence_adjustment IS NULL
            AND importance_adjustment IS NULL
            AND exact_identity_bonus IS NULL
        )
        OR (
            recency_profile_id IS NOT NULL
            AND recency_profile_version IS NOT NULL
            AND recency_profile_sha256 IS NOT NULL
            AND recency_anchor_at IS NOT NULL
            AND recency_age_us IS NOT NULL
            AND recency_factor IS NOT NULL
            AND confidence_factor IS NOT NULL
            AND importance_factor IS NOT NULL
            AND temporal_adjustment IS NOT NULL
            AND confidence_adjustment IS NOT NULL
            AND importance_adjustment IS NOT NULL
            AND exact_identity_bonus IS NOT NULL
        )
    );

-- PostgreSQL round(numeric) is half-away from zero. Temporal receipts instead
-- use this exact integer quotient/remainder/parity helper as an independent
-- fail-closed check of values already produced by the normative Rust scorer.
CREATE FUNCTION memory.round_half_even_integer_v1(
    numerator numeric,
    denominator numeric
)
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
DECLARE
    magnitude_numerator numeric;
    magnitude_denominator numeric;
    quotient numeric;
    remainder numeric;
    complement numeric;
    negative boolean;
BEGIN
    IF denominator = 0 THEN
        RAISE EXCEPTION 'half-even denominator cannot be zero'
            USING ERRCODE = '22012';
    END IF;
    IF numerator <> trunc(numerator) OR denominator <> trunc(denominator) THEN
        RAISE EXCEPTION 'half-even inputs must be integers'
            USING ERRCODE = '22023';
    END IF;

    negative := (numerator < 0) <> (denominator < 0);
    magnitude_numerator := abs(numerator);
    magnitude_denominator := abs(denominator);
    quotient := trunc(magnitude_numerator / magnitude_denominator);
    remainder := mod(magnitude_numerator, magnitude_denominator);
    complement := magnitude_denominator - remainder;

    IF remainder > complement
        OR (remainder = complement AND mod(quotient, 2) = 1)
    THEN
        quotient := quotient + 1;
    END IF;

    IF negative THEN
        RETURN -quotient;
    END IF;
    RETURN quotient;
END;
$$;

DO $$
BEGIN
    IF memory.round_half_even_integer_v1(25, 10) <> 2
        OR memory.round_half_even_integer_v1(35, 10) <> 4
        OR memory.round_half_even_integer_v1(-25, 10) <> -2
        OR memory.round_half_even_integer_v1(-35, 10) <> -4
        OR memory.round_half_even_integer_v1(24, 10) <> 2
        OR memory.round_half_even_integer_v1(26, 10) <> 3
    THEN
        RAISE EXCEPTION 'half-even integer helper failed registration vectors'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

-- Recompute the policy-owned recency curve with exact numeric/integer
-- operations. Rust remains normative; this independent persistence check
-- prevents a plausible but incorrect factor from becoming durable evidence.
CREATE FUNCTION memory.temporal_recency_factor_units_v1(
    requested_profile_id text,
    requested_profile_version text,
    age_us numeric
)
RETURNS numeric
LANGUAGE plpgsql
STABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, memory
AS $$
DECLARE
    profile_document jsonb;
    half_life_us numeric;
    floor_q63_units numeric;
    q63_scale_units numeric;
    factor_q63_units numeric;
    remainder_us numeric;
    whole_half_lives integer;
    constant_units numeric;
BEGIN
    IF age_us < 0 OR age_us <> trunc(age_us) THEN
        RAISE EXCEPTION 'recency age must be a nonnegative integer'
            USING ERRCODE = '22023';
    END IF;

    SELECT profile.profile_document
    INTO profile_document
    FROM memory.recency_profiles AS profile
    WHERE profile.profile_id = requested_profile_id
      AND profile.profile_version = requested_profile_version;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'recency profile is unavailable'
            USING ERRCODE = '23503';
    END IF;

    IF requested_profile_id = 'stable-v1'
        AND requested_profile_version = '1'
    THEN
        RETURN 1000000000000;
    END IF;
    IF requested_profile_id <> 'active-case-30d-v1'
        OR requested_profile_version <> '1'
    THEN
        RAISE EXCEPTION 'recency profile is unsupported'
            USING ERRCODE = '22023';
    END IF;

    half_life_us := (profile_document ->> 'half_life_us')::numeric;
    floor_q63_units := (profile_document ->> 'floor_q63_units')::numeric;
    q63_scale_units := (profile_document ->> 'q63_scale_units')::numeric;
    IF age_us >= half_life_us * 3 THEN
        RETURN 125000000000;
    END IF;

    whole_half_lives := trunc(age_us / half_life_us)::integer;
    factor_q63_units := CASE whole_half_lives
        WHEN 0 THEN q63_scale_units
        WHEN 1 THEN q63_scale_units / 2
        WHEN 2 THEN q63_scale_units / 4
        ELSE floor_q63_units
    END;
    remainder_us := mod(age_us, half_life_us);

    FOR constant_units IN
        SELECT constant.value::numeric
        FROM jsonb_array_elements_text(
            profile_document -> 'q63_constants'
        ) WITH ORDINALITY AS constant(value, ordinality)
        ORDER BY constant.ordinality
    LOOP
        remainder_us := remainder_us * 2;
        IF remainder_us >= half_life_us THEN
            remainder_us := remainder_us - half_life_us;
            factor_q63_units := memory.round_half_even_integer_v1(
                factor_q63_units * constant_units,
                q63_scale_units
            );
        END IF;
    END LOOP;

    factor_q63_units := greatest(factor_q63_units, floor_q63_units);
    RETURN memory.round_half_even_integer_v1(
        factor_q63_units * 1000000000000,
        q63_scale_units
    );
END;
$$;

CREATE OR REPLACE FUNCTION memory.validate_retrieval_manifest_item_channels()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
DECLARE
    receipt_embedding_profile_sha256 character(64);
    receipt_projection_profile_sha256 character(64);
    receipt_query_vector_sha256 character(64);
    receipt_mode text;
    receipt_scoring_mode text;
    receipt_policy_id text;
    receipt_valid_at timestamptz;
    receipt_rrf_k integer;
    receipt_score_scale integer;
    governed_recency_profile_id text;
    governed_recency_profile_version text;
    governed_recency_profile_sha256 character(64);
    governed_recency_anchor_at timestamptz;
    governed_importance numeric(5, 4);
    revision_confidence numeric(5, 4);
    score_scale_units constant numeric := 1000000000000;
    expected_exact_rrf_units numeric;
    expected_lexical_rrf_units numeric;
    expected_vector_rrf_units numeric;
    fused_score_units numeric;
    after_temporal_units numeric;
    after_confidence_units numeric;
    after_importance_units numeric;
    exact_identity_bonus_units numeric;
    expected_final_score_units numeric;
BEGIN
    SELECT
        receipt.embedding_profile_sha256,
        receipt.embedding_projection_profile_sha256,
        receipt.query_vector_sha256,
        policy.retrieval_mode,
        policy.scoring_mode,
        policy.policy_id,
        receipt.valid_at,
        (policy.policy_document #>> '{fusion,k}')::integer,
        (policy.policy_document ->> 'score_scale')::integer
    INTO
        receipt_embedding_profile_sha256,
        receipt_projection_profile_sha256,
        receipt_query_vector_sha256,
        receipt_mode,
        receipt_scoring_mode,
        receipt_policy_id,
        receipt_valid_at,
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
        (
            NEW.exact_rank IS NULL
            AND NEW.lexical_rank IS NULL
            AND NEW.vector_rank IS NULL
        )
        OR (NEW.exact_rank IS NULL) <> (NEW.exact_rrf_contribution = 0)
        OR (NEW.lexical_rank IS NULL) <> (NEW.lexical_rrf_contribution = 0)
        OR (NEW.vector_rank IS NULL) <> (NEW.vector_rrf_contribution = 0)
        OR NEW.fused_score IS NULL
        OR NEW.embedding_profile_sha256
            IS DISTINCT FROM receipt_embedding_profile_sha256
        OR NEW.embedding_projection_profile_sha256
            IS DISTINCT FROM receipt_projection_profile_sha256
        OR NEW.embedding_input_sha256 IS NULL
        OR NEW.embedding_vector_sha256 IS NULL
        OR receipt_query_vector_sha256 IS NULL
        OR NEW.fused_score IS DISTINCT FROM (
            NEW.exact_rrf_contribution
            + NEW.lexical_rrf_contribution
            + NEW.vector_rrf_contribution
        )
    ) THEN
        RAISE EXCEPTION 'hybrid manifest fusion lineage is invalid'
            USING ERRCODE = '23514';
    ELSIF receipt_mode = 'hybrid'
        AND receipt_scoring_mode = 'channel-only'
        AND (
            NEW.final_score <> NEW.fused_score
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
        )
    THEN
        RAISE EXCEPTION 'channel-only hybrid manifest score is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF receipt_scoring_mode = 'channel-only' AND (
        NEW.recency_profile_id IS NOT NULL
        OR NEW.recency_profile_version IS NOT NULL
        OR NEW.recency_profile_sha256 IS NOT NULL
        OR NEW.recency_anchor_at IS NOT NULL
        OR NEW.recency_age_us IS NOT NULL
        OR NEW.recency_factor IS NOT NULL
        OR NEW.confidence_factor IS NOT NULL
        OR NEW.importance_factor IS NOT NULL
        OR NEW.temporal_adjustment IS NOT NULL
        OR NEW.confidence_adjustment IS NOT NULL
        OR NEW.importance_adjustment IS NOT NULL
        OR NEW.exact_identity_bonus IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'channel-only manifest cannot contain temporal lineage'
            USING ERRCODE = '23514';
    ELSIF receipt_scoring_mode = 'temporal-v1' THEN
        IF receipt_policy_id <> 'retrieval-hybrid-temporal-v1'
            OR receipt_mode <> 'hybrid'
            OR NEW.schema_version <> 2
            OR NEW.recency_profile_id IS NULL
            OR NEW.recency_profile_version IS NULL
            OR NEW.recency_profile_sha256 IS NULL
            OR NEW.recency_anchor_at IS NULL
            OR NEW.recency_age_us IS NULL
            OR NEW.recency_factor IS NULL
            OR NEW.confidence_factor IS NULL
            OR NEW.importance_factor IS NULL
            OR NEW.temporal_adjustment IS NULL
            OR NEW.confidence_adjustment IS NULL
            OR NEW.importance_adjustment IS NULL
            OR NEW.exact_identity_bonus IS NULL
        THEN
            RAISE EXCEPTION 'temporal manifest lineage is incomplete'
                USING ERRCODE = '23514';
        END IF;

        SELECT
            governance.recency_profile_id,
            governance.recency_profile_version,
            governance.recency_profile_sha256,
            governance.recency_anchor_at,
            governance.importance,
            revision.confidence
        INTO
            governed_recency_profile_id,
            governed_recency_profile_version,
            governed_recency_profile_sha256,
            governed_recency_anchor_at,
            governed_importance,
            revision_confidence
        FROM memory.fact_revision_governance AS governance
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = governance.tenant_id
         AND revision.subject_id = governance.subject_id
         AND revision.case_id = governance.case_id
         AND revision.fact_id = governance.fact_id
         AND revision.revision_id = governance.revision_id
        WHERE governance.tenant_id = NEW.tenant_id
          AND governance.subject_id = NEW.subject_id
          AND governance.case_id = NEW.case_id
          AND governance.fact_id = NEW.fact_id
          AND governance.revision_id = NEW.revision_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'temporal manifest governance is unavailable'
                USING ERRCODE = '23503';
        END IF;

        IF NEW.recency_profile_id IS DISTINCT FROM governed_recency_profile_id
            OR NEW.recency_profile_version
                IS DISTINCT FROM governed_recency_profile_version
            OR NEW.recency_profile_sha256
                IS DISTINCT FROM governed_recency_profile_sha256
            OR NEW.recency_anchor_at IS DISTINCT FROM governed_recency_anchor_at
            OR NEW.recency_age_us IS DISTINCT FROM greatest(
                0::numeric,
                extract(epoch FROM (
                    receipt_valid_at - governed_recency_anchor_at
                )) * 1000000
            )::numeric(30, 0)
            OR NEW.confidence_factor
                IS DISTINCT FROM revision_confidence::numeric(20, 12)
            OR NEW.importance_factor IS DISTINCT FROM (
                0.5 + governed_importance
            )::numeric(20, 12)
            OR NEW.recency_factor * score_scale_units IS DISTINCT FROM
                memory.temporal_recency_factor_units_v1(
                    NEW.recency_profile_id,
                    NEW.recency_profile_version,
                    NEW.recency_age_us
                )
            OR NOT (
                (
                    NEW.recency_profile_id = 'stable-v1'
                    AND NEW.recency_profile_version = '1'
                    AND NEW.recency_factor = 1.000000000000
                )
                OR (
                    NEW.recency_profile_id = 'active-case-30d-v1'
                    AND NEW.recency_profile_version = '1'
                    AND NEW.recency_factor BETWEEN
                        0.125000000000 AND 1.000000000000
                )
            )
            OR NEW.exact_identity_bonus IS DISTINCT FROM (CASE
                WHEN NEW.exact_identity_rank = 1 THEN 0.016393442623
                WHEN NEW.exact_identity_rank = 2 THEN 0.008196721311
                ELSE 0.000000000000
            END)
        THEN
            RAISE EXCEPTION 'temporal manifest governance or score is invalid'
                USING ERRCODE = '23514';
        END IF;

        expected_exact_rrf_units := CASE
            WHEN NEW.exact_rank IS NULL THEN 0
            ELSE memory.round_half_even_integer_v1(
                score_scale_units,
                receipt_rrf_k + NEW.exact_rank
            )
        END;
        expected_lexical_rrf_units := CASE
            WHEN NEW.lexical_rank IS NULL THEN 0
            ELSE memory.round_half_even_integer_v1(
                score_scale_units,
                receipt_rrf_k + NEW.lexical_rank
            )
        END;
        expected_vector_rrf_units := CASE
            WHEN NEW.vector_rank IS NULL THEN 0
            ELSE memory.round_half_even_integer_v1(
                score_scale_units,
                receipt_rrf_k + NEW.vector_rank
            )
        END;
        fused_score_units := NEW.fused_score * score_scale_units;
        after_temporal_units := memory.round_half_even_integer_v1(
            fused_score_units * NEW.recency_factor * score_scale_units,
            score_scale_units
        );
        after_confidence_units := memory.round_half_even_integer_v1(
            after_temporal_units * NEW.confidence_factor * score_scale_units,
            score_scale_units
        );
        after_importance_units := memory.round_half_even_integer_v1(
            after_confidence_units * NEW.importance_factor * score_scale_units,
            score_scale_units
        );
        exact_identity_bonus_units := CASE
            WHEN NEW.exact_identity_rank = 1 THEN 16393442623
            WHEN NEW.exact_identity_rank = 2 THEN 8196721311
            ELSE 0
        END;
        expected_final_score_units :=
            after_importance_units + exact_identity_bonus_units;

        IF NEW.exact_rrf_contribution * score_scale_units
                IS DISTINCT FROM expected_exact_rrf_units
            OR NEW.lexical_rrf_contribution * score_scale_units
                IS DISTINCT FROM expected_lexical_rrf_units
            OR NEW.vector_rrf_contribution * score_scale_units
                IS DISTINCT FROM expected_vector_rrf_units
            OR fused_score_units IS DISTINCT FROM (
                expected_exact_rrf_units
                + expected_lexical_rrf_units
                + expected_vector_rrf_units
            )
            OR NEW.temporal_adjustment * score_scale_units
                IS DISTINCT FROM after_temporal_units - fused_score_units
            OR NEW.confidence_adjustment * score_scale_units
                IS DISTINCT FROM after_confidence_units - after_temporal_units
            OR NEW.importance_adjustment * score_scale_units
                IS DISTINCT FROM after_importance_units - after_confidence_units
            OR NEW.exact_identity_bonus * score_scale_units
                IS DISTINCT FROM exact_identity_bonus_units
            OR NEW.final_score * score_scale_units
                IS DISTINCT FROM expected_final_score_units
        THEN
            RAISE EXCEPTION 'temporal manifest half-even score is invalid'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

-- Keep authorization-first replay additive: old columns retain their order and
-- the temporal explanation is appended only after all pre-0008 fields.
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
    item.embedding_vector_sha256,
    item.recency_profile_id,
    item.recency_profile_version,
    item.recency_profile_sha256,
    item.recency_anchor_at,
    item.recency_age_us,
    item.recency_factor,
    item.confidence_factor,
    item.importance_factor,
    item.temporal_adjustment,
    item.confidence_adjustment,
    item.importance_adjustment,
    item.exact_identity_bonus
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
  AND revision.sensitivity = ANY (
      SELECT allowed.label
      FROM jsonb_array_elements_text(
          COALESCE(
              NULLIF(
                  current_setting(
                      'palimpsest.allowed_sensitivities',
                      true
                  ),
                  ''
              )::jsonb,
              '[]'::jsonb
          )
      ) AS allowed(label)
  )
  AND item.source_content_sha256 = revision.content_sha256;
