-- 0021: precomputed authorized-current projection (ADR-0032, issue #43)
--
-- An incrementally maintained, tenant-scoped materialization of the
-- authorized current view: authorization (lifecycle), retention, validity,
-- sensitivity, and search-document readiness are applied at write time so
-- per-query retrieval skips the full-set pipeline (authorized-set
-- materialization + governance join + per-row projection verification) that
-- measured as the p95 11.302 s floor at 1,000,000 revisions (spec 002 A4).
--
-- Maintenance is bounded per write (row-level upsert/sync triggers); the
-- durable coverage marker is reconciled per statement (the same cost profile
-- as the existing fact_revision_current coverage, migration 0020) and gives
-- bounded, observable staleness (spec 002 A5d). The structure is reproducible
-- from canonical records via the owner-only rebuild function (constitution
-- principle 12). Retrieval falls back to the canonical pipeline whenever the
-- marker is not complete, so correctness (A5b) and tenant isolation (A5c)
-- semantics are unchanged.

CREATE TABLE memory.authorized_current_projection (
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
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('active', 'deletion_pending', 'deleted')
    ),
    retention_expires_at timestamptz CHECK (
        retention_expires_at IS NULL OR isfinite(retention_expires_at)
    ),
    projection_schema_version integer CHECK (projection_schema_version > 0),
    projection_schema_sha256 character(64) CHECK (
        projection_schema_sha256 IS NULL
        OR projection_schema_sha256 ~ '^[0-9a-f]{64}$'
    ),
    source_content_sha256 character(64) CHECK (
        source_content_sha256 IS NULL
        OR source_content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    projection_sha256 character(64) CHECK (
        projection_sha256 IS NULL OR projection_sha256 ~ '^[0-9a-f]{64}$'
    ),
    search_vector tsvector,
    projection_ready boolean NOT NULL DEFAULT false,
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(authorized_at)),
    PRIMARY KEY (tenant_id, subject_id, fact_id),
    CONSTRAINT authorized_current_projection_revision_fkey
    FOREIGN KEY (tenant_id, subject_id, case_id, fact_id, revision_id)
        REFERENCES memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ) ON DELETE CASCADE
);

CREATE INDEX authorized_current_projection_scope_idx
    ON memory.authorized_current_projection (
        tenant_id, subject_id, case_id, fact_id
    );

-- The per-row GIN search vector drives the fast-path lexical query; the
-- same vector is verified against the canonical projection at write time.
CREATE INDEX authorized_current_projection_gin_idx
    ON memory.authorized_current_projection
    USING gin (search_vector);

-- Write-time readiness verification, shared by the populate, sync, and
-- rebuild functions. Mirrors the per-query verification the canonical
-- pipeline performs in `projected` (migration 0006): the document must
-- exist, its source hash must match the revision content, and its
-- projection digest and search vector must be reproducible from the
-- canonical namespace/key/value (constitution principle 12).
CREATE FUNCTION memory.authorized_current_projection_ready(
    candidate_namespace text,
    candidate_fact_key text,
    candidate_value jsonb,
    candidate_content_sha256 character(64),
    candidate_source_content_sha256 character(64),
    candidate_projection_sha256 character(64),
    candidate_search_vector tsvector
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT candidate_source_content_sha256 IS NOT NULL
        AND candidate_source_content_sha256 = candidate_content_sha256
        AND candidate_projection_sha256 = memory.fact_projection_sha256_v1(
            candidate_namespace, candidate_fact_key, candidate_value
        )
        AND candidate_search_vector = memory.fact_search_vector_v1(
            candidate_namespace, candidate_fact_key, candidate_value
        )
$$;

CREATE FUNCTION memory.populate_authorized_current_projection_bulk()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    -- transition-table joins are estimated poorly (no statistics);
    -- force hash/merge joins so bulk statements stay O(N log N)
    SET LOCAL enable_nestloop = off;
    SET LOCAL work_mem = '64MB';

    INSERT INTO memory.authorized_current_projection AS existing (
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
        schema_version,
        lifecycle_state,
        retention_expires_at,
        projection_schema_version,
        projection_schema_sha256,
        source_content_sha256,
        projection_sha256,
        search_vector,
        projection_ready,
        authorized_at
    )
    SELECT
        current_row.tenant_id,
        current_row.subject_id,
        current_row.case_id,
        current_row.fact_id,
        current_row.revision_id,
        current_row.revision_no,
        current_row.observed_at,
        current_row.recorded_at,
        current_row.valid_during,
        fact.namespace,
        fact.fact_key,
        current_row.value,
        current_row.confidence,
        current_row.sensitivity,
        current_row.content_sha256,
        current_row.schema_version,
        governance.lifecycle_state,
        governance.retention_expires_at,
        document.projection_schema_version,
        document.projection_schema_sha256,
        document.source_content_sha256,
        document.projection_sha256,
        document.search_vector,
        memory.authorized_current_projection_ready(
            fact.namespace,
            fact.fact_key,
            current_row.value,
            current_row.content_sha256,
            document.source_content_sha256,
            document.projection_sha256,
            document.search_vector
        ),
        clock_timestamp()
    FROM new_revisions AS current_row
    JOIN memory.facts AS fact
      ON fact.tenant_id = current_row.tenant_id
     AND fact.subject_id = current_row.subject_id
     AND fact.case_id = current_row.case_id
     AND fact.fact_id = current_row.fact_id
    JOIN memory.fact_revision_governance AS governance
      ON governance.tenant_id = current_row.tenant_id
     AND governance.subject_id = current_row.subject_id
     AND governance.case_id = current_row.case_id
     AND governance.fact_id = current_row.fact_id
     AND governance.revision_id = current_row.revision_id
    LEFT JOIN memory.fact_revision_search_documents AS document
      ON document.tenant_id = current_row.tenant_id
     AND document.subject_id = current_row.subject_id
     AND document.case_id = current_row.case_id
     AND document.fact_id = current_row.fact_id
     AND document.revision_id = current_row.revision_id
    ON CONFLICT (tenant_id, subject_id, fact_id) DO UPDATE
    SET revision_id = EXCLUDED.revision_id,
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
        schema_version = EXCLUDED.schema_version,
        lifecycle_state = EXCLUDED.lifecycle_state,
        retention_expires_at = EXCLUDED.retention_expires_at,
        projection_schema_version = EXCLUDED.projection_schema_version,
        projection_schema_sha256 = EXCLUDED.projection_schema_sha256,
        source_content_sha256 = EXCLUDED.source_content_sha256,
        projection_sha256 = EXCLUDED.projection_sha256,
        search_vector = EXCLUDED.search_vector,
        projection_ready = EXCLUDED.projection_ready,
        authorized_at = EXCLUDED.authorized_at
    WHERE existing.revision_no < EXCLUDED.revision_no
       OR (
           existing.revision_no = EXCLUDED.revision_no
           AND existing.revision_id < EXCLUDED.revision_id
       );
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.populate_authorized_current_projection_bulk()
FROM PUBLIC;

-- The z-prefix places this statement trigger after the 0006 metadata
-- row trigger (governance + documents) and 0017 populate_current, so the
-- transition table join sees the complete per-revision context. Fires once
-- per statement, so a bulk seed reconciles once, not once per row.
CREATE TRIGGER fact_revisions_z_populate_authorized_current_projection
AFTER INSERT ON memory.fact_revisions
REFERENCING NEW TABLE AS new_revisions
FOR EACH STATEMENT
EXECUTE FUNCTION memory.populate_authorized_current_projection_bulk();

CREATE FUNCTION memory.sync_authorized_current_projection_governance_bulk()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    -- transition-table joins are estimated poorly (no statistics);
    -- force hash/merge joins so bulk statements stay O(N log N)
    SET LOCAL enable_nestloop = off;
    SET LOCAL work_mem = '64MB';

    UPDATE memory.authorized_current_projection AS projection
    SET lifecycle_state = governance.lifecycle_state,
        retention_expires_at = governance.retention_expires_at,
        authorized_at = clock_timestamp()
    FROM new_governance AS governance
    WHERE governance.tenant_id = projection.tenant_id
      AND governance.subject_id = projection.subject_id
      AND governance.fact_id = projection.fact_id
      AND governance.revision_id = projection.revision_id;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.sync_authorized_current_projection_governance_bulk()
FROM PUBLIC;

-- Governance is inserted by populate_fact_revision_retrieval_metadata within
-- the fact_revisions statement; lifecycle_state later transitions through
-- the deletion fence. Both flow into the structure row here, statement-wide.
CREATE TRIGGER fact_revision_governance_sync_authorized_current_projection_insert
AFTER INSERT ON memory.fact_revision_governance
REFERENCING NEW TABLE AS new_governance
FOR EACH STATEMENT
EXECUTE FUNCTION memory.sync_authorized_current_projection_governance_bulk();

CREATE TRIGGER fact_revision_governance_sync_authorized_current_projection_update
AFTER UPDATE ON memory.fact_revision_governance
REFERENCING NEW TABLE AS new_governance
FOR EACH STATEMENT
EXECUTE FUNCTION memory.sync_authorized_current_projection_governance_bulk();

CREATE FUNCTION memory.sync_authorized_current_projection_documents_bulk()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    -- transition-table joins are estimated poorly (no statistics);
    -- force hash/merge joins so bulk statements stay O(N log N)
    SET LOCAL enable_nestloop = off;
    SET LOCAL work_mem = '64MB';

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        -- NEW rows (insert, update, re-projection): copy the verified
        -- document fields and recompute readiness against the canonical
        -- projection.
        UPDATE memory.authorized_current_projection AS projection
        SET projection_schema_version = document.projection_schema_version,
            projection_schema_sha256 = document.projection_schema_sha256,
            source_content_sha256 = document.source_content_sha256,
            projection_sha256 = document.projection_sha256,
            search_vector = document.search_vector,
            projection_ready = memory.authorized_current_projection_ready(
                projection.namespace,
                projection.fact_key,
                projection.value,
                projection.content_sha256,
                document.source_content_sha256,
                document.projection_sha256,
                document.search_vector
            ),
            authorized_at = clock_timestamp()
        FROM new_documents AS document
        WHERE document.tenant_id = projection.tenant_id
          AND document.subject_id = projection.subject_id
          AND document.case_id = projection.case_id
          AND document.fact_id = projection.fact_id
          AND document.revision_id = projection.revision_id;
    END IF;

    IF TG_OP = 'DELETE' THEN
        -- OLD rows whose document was removed: the structure row must not
        -- stay ready, so corruption and deletion fail closed exactly as the
        -- canonical per-query verification does. (A DELETE trigger cannot
        -- declare NEW TABLE, and for UPDATE the NEW pass above re-syncs the
        -- same primary-keyed structure row, so no exclusion is needed.)
        UPDATE memory.authorized_current_projection AS projection
        SET projection_schema_version = NULL,
            projection_schema_sha256 = NULL,
            source_content_sha256 = NULL,
            projection_sha256 = NULL,
            search_vector = NULL,
            projection_ready = false,
            authorized_at = clock_timestamp()
        FROM old_documents AS document
        WHERE document.tenant_id = projection.tenant_id
          AND document.subject_id = projection.subject_id
          AND document.case_id = projection.case_id
          AND document.fact_id = projection.fact_id
          AND document.revision_id = projection.revision_id;
    END IF;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.sync_authorized_current_projection_documents_bulk()
FROM PUBLIC;

-- Direct document mutation (corruption, deletion, re-projection, restore)
-- must propagate into the structure so readiness fails closed exactly as the
-- canonical per-query verification does. Transition-table, statement-wide.
CREATE TRIGGER search_documents_sync_authorized_current_projection_insert
AFTER INSERT ON memory.fact_revision_search_documents
REFERENCING NEW TABLE AS new_documents
FOR EACH STATEMENT
EXECUTE FUNCTION memory.sync_authorized_current_projection_documents_bulk();

CREATE TRIGGER search_documents_sync_authorized_current_projection_update
AFTER UPDATE ON memory.fact_revision_search_documents
REFERENCING NEW TABLE AS new_documents
            OLD TABLE AS old_documents
FOR EACH STATEMENT
EXECUTE FUNCTION memory.sync_authorized_current_projection_documents_bulk();

CREATE TRIGGER search_documents_sync_authorized_current_projection_delete
AFTER DELETE ON memory.fact_revision_search_documents
REFERENCING OLD TABLE AS old_documents
FOR EACH STATEMENT
EXECUTE FUNCTION memory.sync_authorized_current_projection_documents_bulk();

ALTER TABLE memory.authorized_current_projection ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.authorized_current_projection FORCE ROW LEVEL SECURITY;

CREATE POLICY authorized_current_projection_select_scope
    ON memory.authorized_current_projection
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_insert_scope
    ON memory.authorized_current_projection
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_update_scope
    ON memory.authorized_current_projection
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_active_subject
    ON memory.authorized_current_projection AS RESTRICTIVE
    FOR ALL
    USING (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    )
    WITH CHECK (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    );

CREATE POLICY authorized_current_projection_deletion_worker_cleanup
    ON memory.authorized_current_projection
    FOR DELETE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND memory.deletion_workflow_allows(tenant_id, subject_id)
    );

CREATE POLICY authorized_current_projection_repair_delete
    ON memory.authorized_current_projection
    FOR DELETE
    USING (
        current_user = pg_get_userbyid((
            SELECT relowner
            FROM pg_catalog.pg_class
            WHERE oid = 'memory.authorized_current_projection'::pg_catalog.regclass
        ))
        AND current_setting('palimpsest.authorized_current_repair', true)
            = 'palimpsest-authorized-current-repair-v1'
        AND tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

-- Durable, scope-local proof for the precomputed structure. A scope is
-- complete only when the structure has one READY row for every canonical
-- fact, every ready row is valid at the check instant, and all ready rows
-- share one projection schema (so the retrieval-policy schema comparison is
-- sound). A finite horizon bounds staleness: retrieval stops trusting the
-- proof once a valid-time interval expires.
CREATE TABLE memory.authorized_current_projection_coverage (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    coverage_state text NOT NULL CHECK (
        coverage_state IN ('complete', 'repair_required')
    ),
    fact_count bigint NOT NULL CHECK (fact_count >= 0),
    projection_count bigint NOT NULL CHECK (projection_count >= 0),
    projection_schema_version_min integer CHECK (
        projection_schema_version_min IS NULL OR projection_schema_version_min > 0
    ),
    projection_schema_version_max integer CHECK (
        projection_schema_version_max IS NULL OR projection_schema_version_max > 0
    ),
    projection_schema_sha256 character(64) CHECK (
        projection_schema_sha256 IS NULL
        OR projection_schema_sha256 ~ '^[0-9a-f]{64}$'
    ),
    coverage_valid_until timestamptz CHECK (
        coverage_valid_until IS NULL OR isfinite(coverage_valid_until)
    ),
    checked_at timestamptz NOT NULL CHECK (isfinite(checked_at)),
    PRIMARY KEY (tenant_id, subject_id)
);

REVOKE ALL ON TABLE memory.authorized_current_projection_coverage FROM PUBLIC;

ALTER TABLE memory.authorized_current_projection_coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.authorized_current_projection_coverage FORCE ROW LEVEL SECURITY;

CREATE POLICY authorized_current_projection_coverage_select_scope
    ON memory.authorized_current_projection_coverage
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_coverage_insert_scope
    ON memory.authorized_current_projection_coverage
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_coverage_update_scope
    ON memory.authorized_current_projection_coverage
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY authorized_current_projection_coverage_delete_scope
    ON memory.authorized_current_projection_coverage
    FOR DELETE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND (
            memory.deletion_workflow_allows(tenant_id, subject_id)
            OR current_setting(
                'palimpsest.authorized_current_coverage_maintenance',
                true
            ) = 'palimpsest-authorized-current-coverage-v1'
        )
    );

CREATE POLICY authorized_current_projection_coverage_active_subject
    ON memory.authorized_current_projection_coverage AS RESTRICTIVE
    FOR ALL
    USING (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    )
    WITH CHECK (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    );

CREATE FUNCTION memory.reconcile_authorized_current_projection_coverage(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    checked_at_value timestamptz := clock_timestamp();
    fact_count_value bigint;
    projection_count_value bigint;
    coverage_valid_until_value timestamptz;
    schema_version_min_value integer;
    schema_version_max_value integer;
    schema_sha256_value character(64);
    coverage_state_value text;
BEGIN
    -- transition-table joins are estimated poorly (no statistics);
    -- force hash/merge joins so bulk statements stay O(N log N)
    SET LOCAL enable_nestloop = off;
    SET LOCAL work_mem = '64MB';

    IF candidate_tenant_id IS NULL OR candidate_subject_id IS NULL THEN
        RAISE EXCEPTION 'authorized-current coverage scope is required'
            USING ERRCODE = '22023';
    END IF;

    SELECT count(*)::bigint
    INTO fact_count_value
    FROM memory.facts
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;

    SELECT count(*)::bigint,
        min(upper(projection.valid_during))
            FILTER (WHERE NOT upper_inf(projection.valid_during)),
        min(projection.projection_schema_version)
            FILTER (WHERE projection.projection_ready),
        max(projection.projection_schema_version)
            FILTER (WHERE projection.projection_ready),
        min(projection.projection_schema_sha256)
            FILTER (WHERE projection.projection_ready)
    INTO projection_count_value,
        coverage_valid_until_value,
        schema_version_min_value,
        schema_version_max_value,
        schema_sha256_value
    FROM memory.authorized_current_projection AS projection
    WHERE projection.tenant_id = candidate_tenant_id
      AND projection.subject_id = candidate_subject_id
      AND projection.projection_ready;

    -- Completeness is a structural property: every fact must have exactly
    -- one ready, schema-conformant structure row. Temporal effectiveness
    -- (valid_during @> now, recorded_at <= now) is a per-query predicate
    -- applied identically by the fast path and the canonical fallback, so
    -- future-valid revisions (e.g. a not-yet-effective successor) must not
    -- force repair_required — the canonical pipeline likewise serves them
    -- through the fallback exclusion rather than failing closed.
    coverage_state_value := CASE
        WHEN fact_count_value = projection_count_value
             AND (
                 schema_version_min_value = schema_version_max_value
                 OR (
                     schema_version_min_value IS NULL
                     AND schema_version_max_value IS NULL
                 )
             )
            THEN 'complete'
        ELSE 'repair_required'
    END;

    INSERT INTO memory.authorized_current_projection_coverage (
        tenant_id,
        subject_id,
        coverage_state,
        fact_count,
        projection_count,
        projection_schema_version_min,
        projection_schema_version_max,
        projection_schema_sha256,
        coverage_valid_until,
        checked_at
    )
    VALUES (
        candidate_tenant_id,
        candidate_subject_id,
        coverage_state_value,
        fact_count_value,
        projection_count_value,
        schema_version_min_value,
        schema_version_max_value,
        schema_sha256_value,
        coverage_valid_until_value,
        checked_at_value
    )
    ON CONFLICT (tenant_id, subject_id) DO UPDATE
    SET coverage_state = EXCLUDED.coverage_state,
        fact_count = EXCLUDED.fact_count,
        projection_count = EXCLUDED.projection_count,
        projection_schema_version_min = EXCLUDED.projection_schema_version_min,
        projection_schema_version_max = EXCLUDED.projection_schema_version_max,
        projection_schema_sha256 = EXCLUDED.projection_schema_sha256,
        coverage_valid_until = EXCLUDED.coverage_valid_until,
        checked_at = EXCLUDED.checked_at;

    RETURN coverage_state_value;
END;
$$;

REVOKE ALL ON FUNCTION memory.reconcile_authorized_current_projection_coverage(uuid, uuid)
FROM PUBLIC;

-- Statement-level reconcile after any structure write. Recomputing per
-- statement (not per row) bounds the reconcile count to a small constant
-- per outer statement (a fact_revisions INSERT can fire up to 2-3: one
-- from the populate INSERT plus one each from the governance/document
-- sync UPDATEs) and makes the marker order-proof: it reflects the
-- structure exactly as committed, regardless of which row-level trigger
-- (populate, governance sync, document sync, cascade) changed rows.
CREATE FUNCTION memory.reconcile_authorized_current_coverage_after_structure_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    scope record;
BEGIN
    -- transition-table joins are estimated poorly (no statistics);
    -- force hash/merge joins so bulk statements stay O(N log N)
    SET LOCAL enable_nestloop = off;
    SET LOCAL work_mem = '64MB';

    FOR scope IN
        SELECT DISTINCT scopes.tenant_id, scopes.subject_id
        FROM scopes
    LOOP
        PERFORM memory.reconcile_authorized_current_projection_coverage(
            scope.tenant_id,
            scope.subject_id
        );
    END LOOP;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.reconcile_authorized_current_coverage_after_structure_write()
FROM PUBLIC;

CREATE TRIGGER authorized_current_projection_reconcile_coverage_after_insert
AFTER INSERT ON memory.authorized_current_projection
REFERENCING NEW TABLE AS scopes
FOR EACH STATEMENT
EXECUTE FUNCTION memory.reconcile_authorized_current_coverage_after_structure_write();

CREATE TRIGGER authorized_current_projection_reconcile_coverage_after_update
AFTER UPDATE ON memory.authorized_current_projection
REFERENCING NEW TABLE AS scopes
FOR EACH STATEMENT
EXECUTE FUNCTION memory.reconcile_authorized_current_coverage_after_structure_write();

CREATE TRIGGER authorized_current_projection_reconcile_coverage_after_delete
AFTER DELETE ON memory.authorized_current_projection
REFERENCING OLD TABLE AS scopes
FOR EACH STATEMENT
EXECUTE FUNCTION memory.reconcile_authorized_current_coverage_after_structure_write();

-- Canonical-fact deletions that cascade away structure rows (restore purge,
-- subject lifecycle) land here; orphan facts without a structure row still
-- invalidate the proof so a stale 'complete' can never outlive a purge.
CREATE FUNCTION memory.mark_authorized_current_projection_coverage_for_fact_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    PERFORM set_config(
        'palimpsest.authorized_current_coverage_maintenance',
        'palimpsest-authorized-current-coverage-v1',
        true
    );

    DELETE FROM memory.authorized_current_projection_coverage AS coverage
    USING (
        SELECT DISTINCT deleted.tenant_id, deleted.subject_id
        FROM deleted
    ) AS scope
    WHERE coverage.tenant_id = scope.tenant_id
      AND coverage.subject_id = scope.subject_id
      AND NOT EXISTS (
          SELECT 1
          FROM memory.facts AS fact
          WHERE fact.tenant_id = scope.tenant_id
            AND fact.subject_id = scope.subject_id
      );

    UPDATE memory.authorized_current_projection_coverage AS coverage
    SET coverage_state = 'repair_required',
        checked_at = clock_timestamp()
    FROM (
        SELECT DISTINCT deleted.tenant_id, deleted.subject_id
        FROM deleted
    ) AS scope
    WHERE coverage.tenant_id = scope.tenant_id
      AND coverage.subject_id = scope.subject_id;

    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.mark_authorized_current_projection_coverage_for_fact_delete()
FROM PUBLIC;

CREATE TRIGGER facts_mark_authorized_current_projection_coverage_after_delete
AFTER DELETE ON memory.facts
REFERENCING OLD TABLE AS deleted
FOR EACH STATEMENT
EXECUTE FUNCTION memory.mark_authorized_current_projection_coverage_for_fact_delete();

-- Owner-only rebuild from canonical records (constitution principle 12).
-- Mirrors rebuild_fact_revision_current (migration 0019): the session user
-- must own the table, and the repair scope GUCs are set for the RLS policies.
CREATE FUNCTION memory.rebuild_authorized_current_projection(
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
    WHERE oid = 'memory.authorized_current_projection'::pg_catalog.regclass;

    IF candidate_tenant_id IS NULL
       OR candidate_subject_id IS NULL
       OR session_user <> table_owner THEN
        RAISE EXCEPTION 'authorized-current repair is not authorized'
            USING ERRCODE = '42501';
    END IF;

    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    PERFORM set_config(
        'palimpsest.authorized_current_repair',
        'palimpsest-authorized-current-repair-v1',
        true
    );

    DELETE FROM memory.authorized_current_projection
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;

    INSERT INTO memory.authorized_current_projection (
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
        schema_version,
        lifecycle_state,
        retention_expires_at,
        projection_schema_version,
        projection_schema_sha256,
        source_content_sha256,
        projection_sha256,
        search_vector,
        projection_ready,
        authorized_at
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
        revision.schema_version,
        governance.lifecycle_state,
        governance.retention_expires_at,
        document.projection_schema_version,
        document.projection_schema_sha256,
        document.source_content_sha256,
        document.projection_sha256,
        document.search_vector,
        memory.authorized_current_projection_ready(
            fact.namespace,
            fact.fact_key,
            revision.value,
            revision.content_sha256,
            document.source_content_sha256,
            document.projection_sha256,
            document.search_vector
        ),
        clock_timestamp()
    FROM memory.fact_revisions AS revision
    JOIN memory.facts AS fact
      ON fact.tenant_id = revision.tenant_id
     AND fact.subject_id = revision.subject_id
     AND fact.case_id = revision.case_id
     AND fact.fact_id = revision.fact_id
    LEFT JOIN memory.fact_revision_governance AS governance
      ON governance.tenant_id = revision.tenant_id
     AND governance.subject_id = revision.subject_id
     AND governance.case_id = revision.case_id
     AND governance.fact_id = revision.fact_id
     AND governance.revision_id = revision.revision_id
    LEFT JOIN memory.fact_revision_search_documents AS document
      ON document.tenant_id = revision.tenant_id
     AND document.subject_id = revision.subject_id
     AND document.case_id = revision.case_id
     AND document.fact_id = revision.fact_id
     AND document.revision_id = revision.revision_id
    WHERE revision.tenant_id = candidate_tenant_id
      AND revision.subject_id = candidate_subject_id
    ORDER BY revision.tenant_id, revision.subject_id, revision.fact_id,
        revision.revision_no DESC, revision.revision_id;

    GET DIAGNOSTICS rebuilt = ROW_COUNT;
    PERFORM memory.reconcile_authorized_current_projection_coverage(
        candidate_tenant_id,
        candidate_subject_id
    );
    RETURN rebuilt;
END;
$$;

REVOKE ALL ON FUNCTION memory.rebuild_authorized_current_projection(uuid, uuid)
FROM PUBLIC;

-- Restore purge integration: mirror the migration 0020 rename chain so a
-- restore replay also removes the structure and its proof. The structure
-- rows cascade away with fact_revisions; the explicit delete is idempotent
-- and keeps the purge self-contained.
ALTER FUNCTION memory.restore_purge_scope(uuid, uuid)
    RENAME TO restore_purge_scope_without_authorized_current;

CREATE FUNCTION memory.restore_purge_scope(
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
    residual := memory.restore_purge_scope_without_authorized_current(
        candidate_tenant_id,
        candidate_subject_id
    );
    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    PERFORM set_config(
        'palimpsest.authorized_current_coverage_maintenance',
        'palimpsest-authorized-current-coverage-v1',
        true
    );
    DELETE FROM memory.authorized_current_projection
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;
    DELETE FROM memory.authorized_current_projection_coverage
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;
    RETURN residual;
END;
$$;

REVOKE ALL ON FUNCTION memory.restore_purge_scope_without_authorized_current(uuid, uuid)
FROM PUBLIC;
REVOKE ALL ON FUNCTION memory.restore_purge_scope(uuid, uuid)
FROM PUBLIC;
