-- Keep a durable, scope-local proof for the current fact-revision projection.
-- The proof is deliberately conservative: a scope is complete only when the
-- projection has one row for every canonical fact and every projected row is
-- valid at the check instant. A finite horizon lets retrieval stop trusting
-- the proof once a valid-time interval expires.

CREATE TABLE memory.fact_revision_current_coverage (
    tenant_id uuid NOT NULL,
    subject_id uuid NOT NULL,
    coverage_state text NOT NULL CHECK (
        coverage_state IN ('complete', 'repair_required')
    ),
    fact_count bigint NOT NULL CHECK (fact_count >= 0),
    projection_count bigint NOT NULL CHECK (projection_count >= 0),
    coverage_valid_until timestamptz CHECK (
        coverage_valid_until IS NULL OR isfinite(coverage_valid_until)
    ),
    checked_at timestamptz NOT NULL CHECK (isfinite(checked_at)),
    PRIMARY KEY (tenant_id, subject_id)
);

REVOKE ALL ON TABLE memory.fact_revision_current_coverage FROM PUBLIC;

WITH checked AS MATERIALIZED (
    SELECT clock_timestamp() AS checked_at
),
scopes AS (
    SELECT tenant_id, subject_id FROM memory.facts
    UNION
    SELECT tenant_id, subject_id FROM memory.fact_revision_current
),
fact_counts AS (
    SELECT tenant_id, subject_id, count(*)::bigint AS fact_count
    FROM memory.facts
    GROUP BY tenant_id, subject_id
),
projection_counts AS (
    SELECT projection.tenant_id,
        projection.subject_id,
        count(*)::bigint AS projection_count,
        min(upper(projection.valid_during))
            FILTER (WHERE NOT upper_inf(projection.valid_during))
            AS coverage_valid_until,
        COALESCE(
            bool_and(
                projection.recorded_at <= checked.checked_at
                AND projection.valid_during @> checked.checked_at
            ),
            true
        ) AS projection_is_current
    FROM memory.fact_revision_current AS projection
    CROSS JOIN checked
    GROUP BY projection.tenant_id, projection.subject_id
)
INSERT INTO memory.fact_revision_current_coverage (
    tenant_id,
    subject_id,
    coverage_state,
    fact_count,
    projection_count,
    coverage_valid_until,
    checked_at
)
SELECT scopes.tenant_id,
    scopes.subject_id,
    CASE
        WHEN COALESCE(fact_counts.fact_count, 0)
                = COALESCE(projection_counts.projection_count, 0)
             AND COALESCE(projection_counts.projection_is_current, true)
            THEN 'complete'
        ELSE 'repair_required'
    END,
    COALESCE(fact_counts.fact_count, 0),
    COALESCE(projection_counts.projection_count, 0),
    projection_counts.coverage_valid_until,
    checked.checked_at
FROM scopes
LEFT JOIN fact_counts
  USING (tenant_id, subject_id)
LEFT JOIN projection_counts
  USING (tenant_id, subject_id)
CROSS JOIN checked;

ALTER TABLE memory.fact_revision_current_coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.fact_revision_current_coverage FORCE ROW LEVEL SECURITY;

CREATE POLICY fact_revision_current_coverage_select_scope
    ON memory.fact_revision_current_coverage
    FOR SELECT
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY fact_revision_current_coverage_insert_scope
    ON memory.fact_revision_current_coverage
    FOR INSERT
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY fact_revision_current_coverage_update_scope
    ON memory.fact_revision_current_coverage
    FOR UPDATE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
    );

CREATE POLICY fact_revision_current_coverage_delete_scope
    ON memory.fact_revision_current_coverage
    FOR DELETE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND (
            memory.deletion_workflow_allows(tenant_id, subject_id)
            OR current_setting(
                'palimpsest.fact_revision_current_coverage_maintenance',
                true
            ) = 'palimpsest-fact-current-coverage-v1'
        )
    );

CREATE POLICY fact_revision_current_coverage_active_subject
    ON memory.fact_revision_current_coverage AS RESTRICTIVE
    FOR ALL
    USING (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    )
    WITH CHECK (
        memory.subject_lifecycle_allows_content(tenant_id, subject_id)
        OR memory.deletion_workflow_allows(tenant_id, subject_id)
    );

CREATE FUNCTION memory.reconcile_fact_revision_current_coverage(
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
    projection_is_current boolean;
    coverage_state_value text;
BEGIN
    IF candidate_tenant_id IS NULL OR candidate_subject_id IS NULL THEN
        RAISE EXCEPTION 'current fact-revision coverage scope is required'
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
        COALESCE(
            bool_and(
                projection.recorded_at <= checked_at_value
                AND projection.valid_during @> checked_at_value
            ),
            true
        )
    INTO projection_count_value,
        coverage_valid_until_value,
        projection_is_current
    FROM memory.fact_revision_current AS projection
    WHERE projection.tenant_id = candidate_tenant_id
      AND projection.subject_id = candidate_subject_id;

    coverage_state_value := CASE
        WHEN fact_count_value = projection_count_value
             AND projection_is_current
            THEN 'complete'
        ELSE 'repair_required'
    END;

    INSERT INTO memory.fact_revision_current_coverage (
        tenant_id,
        subject_id,
        coverage_state,
        fact_count,
        projection_count,
        coverage_valid_until,
        checked_at
    )
    VALUES (
        candidate_tenant_id,
        candidate_subject_id,
        coverage_state_value,
        fact_count_value,
        projection_count_value,
        coverage_valid_until_value,
        checked_at_value
    )
    ON CONFLICT (tenant_id, subject_id) DO UPDATE
    SET coverage_state = EXCLUDED.coverage_state,
        fact_count = EXCLUDED.fact_count,
        projection_count = EXCLUDED.projection_count,
        coverage_valid_until = EXCLUDED.coverage_valid_until,
        checked_at = EXCLUDED.checked_at;

    RETURN coverage_state_value;
END;
$$;

REVOKE ALL ON FUNCTION memory.reconcile_fact_revision_current_coverage(uuid, uuid)
FROM PUBLIC;

CREATE FUNCTION memory.mark_fact_revision_current_coverage_for_fact_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    WITH checked AS MATERIALIZED (
        SELECT clock_timestamp() AS checked_at
    ),
    scopes AS (
        SELECT DISTINCT inserted.tenant_id, inserted.subject_id
        FROM inserted
    )
    INSERT INTO memory.fact_revision_current_coverage (
        tenant_id, subject_id, coverage_state,
        fact_count, projection_count, checked_at
    )
    SELECT scopes.tenant_id,
        scopes.subject_id,
        'repair_required',
        0,
        0,
        checked.checked_at
    FROM scopes
    CROSS JOIN checked
    ON CONFLICT (tenant_id, subject_id) DO UPDATE
    SET coverage_state = 'repair_required',
        checked_at = EXCLUDED.checked_at;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.mark_fact_revision_current_coverage_for_fact_insert()
FROM PUBLIC;

CREATE TRIGGER facts_mark_fact_revision_current_coverage
AFTER INSERT ON memory.facts
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION memory.mark_fact_revision_current_coverage_for_fact_insert();

CREATE FUNCTION memory.mark_fact_revision_current_coverage_for_fact_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    PERFORM set_config(
        'palimpsest.fact_revision_current_coverage_maintenance',
        'palimpsest-fact-current-coverage-v1',
        true
    );

    DELETE FROM memory.fact_revision_current_coverage AS coverage
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

    UPDATE memory.fact_revision_current_coverage AS coverage
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

REVOKE ALL ON FUNCTION memory.mark_fact_revision_current_coverage_for_fact_delete()
FROM PUBLIC;

CREATE TRIGGER facts_mark_fact_revision_current_coverage_after_delete
AFTER DELETE ON memory.facts
REFERENCING OLD TABLE AS deleted
FOR EACH STATEMENT
EXECUTE FUNCTION memory.mark_fact_revision_current_coverage_for_fact_delete();

CREATE FUNCTION memory.reconcile_fact_revision_current_coverage_after_revision_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    scope record;
BEGIN
    FOR scope IN
        SELECT DISTINCT inserted.tenant_id, inserted.subject_id
        FROM inserted
    LOOP
        PERFORM memory.reconcile_fact_revision_current_coverage(
            scope.tenant_id,
            scope.subject_id
        );
    END LOOP;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.reconcile_fact_revision_current_coverage_after_revision_insert()
FROM PUBLIC;

-- The z-prefix ensures this statement-level trigger runs after the existing
-- row-level trigger that populates fact_revision_current.
CREATE TRIGGER fact_revisions_z_reconcile_fact_revision_current_coverage
AFTER INSERT ON memory.fact_revisions
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION memory.reconcile_fact_revision_current_coverage_after_revision_insert();

CREATE FUNCTION memory.mark_fact_revision_current_coverage_for_projection_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
BEGIN
    WITH checked AS MATERIALIZED (
        SELECT clock_timestamp() AS checked_at
    ),
    scopes AS (
        SELECT DISTINCT deleted.tenant_id, deleted.subject_id
        FROM deleted
    )
    INSERT INTO memory.fact_revision_current_coverage (
        tenant_id, subject_id, coverage_state,
        fact_count, projection_count, checked_at
    )
    SELECT scopes.tenant_id,
        scopes.subject_id,
        'repair_required',
        0,
        0,
        checked.checked_at
    FROM scopes
    CROSS JOIN checked
    ON CONFLICT (tenant_id, subject_id) DO UPDATE
    SET coverage_state = 'repair_required',
        checked_at = EXCLUDED.checked_at;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION memory.mark_fact_revision_current_coverage_for_projection_delete()
FROM PUBLIC;

CREATE TRIGGER fact_revision_current_mark_coverage_repair
AFTER DELETE ON memory.fact_revision_current
REFERENCING OLD TABLE AS deleted
FOR EACH STATEMENT
EXECUTE FUNCTION memory.mark_fact_revision_current_coverage_for_projection_delete();

ALTER FUNCTION memory.rebuild_fact_revision_current(uuid, uuid)
    RENAME TO rebuild_fact_revision_current_without_coverage;

CREATE FUNCTION memory.rebuild_fact_revision_current(
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
BEGIN
    rebuilt := memory.rebuild_fact_revision_current_without_coverage(
        candidate_tenant_id,
        candidate_subject_id
    );
    PERFORM memory.reconcile_fact_revision_current_coverage(
        candidate_tenant_id,
        candidate_subject_id
    );
    RETURN rebuilt;
END;
$$;

REVOKE ALL ON FUNCTION memory.rebuild_fact_revision_current_without_coverage(uuid, uuid)
FROM PUBLIC;
REVOKE ALL ON FUNCTION memory.rebuild_fact_revision_current(uuid, uuid)
FROM PUBLIC;

ALTER FUNCTION memory.restore_purge_scope(uuid, uuid)
    RENAME TO restore_purge_scope_without_current_coverage;

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
    residual := memory.restore_purge_scope_without_current_coverage(
        candidate_tenant_id,
        candidate_subject_id
    );
    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    PERFORM set_config(
        'palimpsest.fact_revision_current_coverage_maintenance',
        'palimpsest-fact-current-coverage-v1',
        true
    );
    DELETE FROM memory.fact_revision_current_coverage
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id;
    RETURN residual;
END;
$$;

REVOKE ALL ON FUNCTION memory.restore_purge_scope_without_current_coverage(uuid, uuid)
FROM PUBLIC;
REVOKE ALL ON FUNCTION memory.restore_purge_scope(uuid, uuid)
FROM PUBLIC;
