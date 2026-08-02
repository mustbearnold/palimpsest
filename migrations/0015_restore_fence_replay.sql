-- Restore a pre-deletion database only through an explicit, privileged replay
-- operation. The independent fence ledger is content-free; scope identifiers
-- are discovered by matching the database's HMAC scope digest, never copied
-- into the ledger or returned in the replay report.

CREATE TABLE memory.restore_replay_receipts (
    ledger_sha256 character(64) PRIMARY KEY CHECK (
        ledger_sha256 ~ '^[0-9a-f]{64}$'
    ),
    profile text NOT NULL CHECK (profile = 'palimpsest-deletion-fence-ledger-v1'),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    scopes_purged bigint NOT NULL CHECK (scopes_purged >= 0),
    residual_rows bigint NOT NULL CHECK (residual_rows = 0),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(completed_at))
);

CREATE TABLE memory.restore_replay_sessions (
    session_id uuid PRIMARY KEY,
    ledger_sha256 character(64) NOT NULL CHECK (
        ledger_sha256 ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at)),
    expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
    completed_at timestamptz
);

REVOKE ALL ON TABLE memory.restore_replay_receipts FROM PUBLIC;
REVOKE ALL ON TABLE memory.restore_replay_sessions FROM PUBLIC;

CREATE FUNCTION memory.restore_replay_allows(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    session_id_text text;
    ledger_sha256_text text;
    allowed boolean;
BEGIN
    session_id_text := NULLIF(current_setting('palimpsest.restore_session_id', true), '');
    ledger_sha256_text := NULLIF(
        current_setting('palimpsest.restore_ledger_sha256', true),
        ''
    );
    IF session_id_text IS NULL OR ledger_sha256_text IS NULL THEN
        RETURN false;
    END IF;
    IF to_regclass('pg_temp.restore_fence_scopes') IS NULL THEN
        RETURN false;
    END IF;

    EXECUTE $query$
        SELECT EXISTS (
            SELECT 1
            FROM memory.restore_replay_sessions AS session
            JOIN pg_temp.restore_fence_scopes AS scope
              ON scope.tenant_id = $1::uuid
             AND scope.subject_id = $2::uuid
            WHERE session.session_id = $3::uuid
              AND rtrim(session.ledger_sha256::text) = $4
              AND session.completed_at IS NULL
              AND session.expires_at > clock_timestamp()
              AND scope.tenant_id = $1::uuid
              AND scope.subject_id = $2::uuid
              AND scope.scope_digest = memory.deletion_scope_digest(
                  $1::uuid,
                  $2::uuid
              )
        )
    $query$
    INTO allowed
    USING candidate_tenant_id, candidate_subject_id, session_id_text, ledger_sha256_text;
    RETURN allowed;
EXCEPTION
    WHEN invalid_text_representation OR invalid_parameter_value OR undefined_table THEN
        RETURN false;
END;
$$;

REVOKE ALL ON FUNCTION memory.restore_replay_allows(uuid, uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION memory.deletion_workflow_allows(
    candidate_tenant_id uuid,
    candidate_subject_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
    SELECT (
        NULLIF(current_setting('palimpsest.deletion_workflow', true), '') IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM memory.deletion_operations AS operation
            JOIN memory.subject_lifecycles AS lifecycle
              USING (tenant_id, subject_id)
            WHERE operation.tenant_id = candidate_tenant_id
              AND operation.subject_id = candidate_subject_id
              AND operation.operation_id =
                  NULLIF(current_setting('palimpsest.deletion_workflow', true), '')::uuid
              AND operation.lifecycle_state IN ('purging', 'verifying')
              AND operation.worker_lease_id IS NOT NULL
              AND lifecycle.lifecycle_state = 'deletion_pending'
        )
    ) OR memory.restore_replay_allows(candidate_tenant_id, candidate_subject_id)
$$;

REVOKE ALL ON FUNCTION memory.deletion_workflow_allows(uuid, uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION memory.restrict_subject_lifecycle_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory, pg_temp
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
        OR (
            OLD.lifecycle_state = 'active'
            AND NEW.lifecycle_state = 'deleted'
            AND memory.restore_replay_allows(OLD.tenant_id, OLD.subject_id)
        )
    ) THEN
        RAISE EXCEPTION 'subject lifecycle transition is invalid'
            USING ERRCODE = '23000';
    END IF;
    IF NEW.lifecycle_state = 'deleted' AND EXISTS (
        SELECT 1
        FROM memory.subject_content_leases AS lease
        WHERE lease.tenant_id = OLD.tenant_id
          AND lease.subject_id = OLD.subject_id
          AND lease.expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'subject lifecycle cannot reach deleted while content leases remain'
            USING ERRCODE = '55000';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

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
    PERFORM set_config('palimpsest.tenant_id', candidate_tenant_id::text, true);
    PERFORM set_config('palimpsest.subject_id', candidate_subject_id::text, true);
    IF NOT memory.restore_replay_allows(candidate_tenant_id, candidate_subject_id) THEN
        RAISE EXCEPTION 'restore replay scope is not authorized'
            USING ERRCODE = '42501';
    END IF;

    DELETE FROM memory.retrieval_manifest_items
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.retrieval_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.retrieval_idempotency_reservations
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.export_manifest_items
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.export_operations
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_evidence
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_governance
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_search_documents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revision_embedding_projections
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.outbox_intents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.idempotency_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.write_audit_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_effect_receipts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_effect_intents
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoint_revisions
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.checkpoints
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.fact_revisions
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.facts
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.episodes
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;
    DELETE FROM memory.subject_content_leases
    WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id;

    UPDATE memory.subject_lifecycles
    SET lifecycle_state = 'deleted',
        state_version = state_version + 1
    WHERE tenant_id = candidate_tenant_id
      AND subject_id = candidate_subject_id
      AND lifecycle_state <> 'deleted';
    IF NOT FOUND THEN
        IF NOT EXISTS (
            SELECT 1
            FROM memory.subject_lifecycles
            WHERE tenant_id = candidate_tenant_id
              AND subject_id = candidate_subject_id
              AND lifecycle_state = 'deleted'
        ) THEN
            RAISE EXCEPTION 'restore replay subject lifecycle is missing'
                USING ERRCODE = 'P0002';
        END IF;
    END IF;

    SELECT
        (SELECT count(*) FROM memory.episodes
         WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.facts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revisions
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_evidence
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_governance
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_search_documents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.fact_revision_embedding_projections
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoints
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_revisions
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_effect_intents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.checkpoint_effect_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.outbox_intents
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.idempotency_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.write_audit_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_receipts
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_manifest_items
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.retrieval_idempotency_reservations
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.export_manifest_items
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.export_operations
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
        + (SELECT count(*) FROM memory.subject_content_leases
           WHERE tenant_id = candidate_tenant_id AND subject_id = candidate_subject_id)
    INTO residual;
    RETURN residual;
END;
$$;

REVOKE ALL ON FUNCTION memory.restore_purge_scope(uuid, uuid) FROM PUBLIC;

CREATE FUNCTION memory.replay_restore_fence_ledger(
    ledger_bytes bytea,
    expected_ledger_sha256 text
)
RETURNS TABLE(
    scopes_found bigint,
    scopes_purged bigint,
    residual_rows bigint,
    ledger_sha256 text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, memory, pg_temp
AS $$
DECLARE
    document jsonb;
    entries jsonb;
    generated_at timestamptz;
    entry_count bigint;
    key_count bigint;
    unsigned_document text;
    replay_session_id uuid;
    scope record;
    residual_scope bigint;
    existing_receipt memory.restore_replay_receipts%ROWTYPE;
BEGIN
    IF expected_ledger_sha256 IS NULL
       OR expected_ledger_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'restore fence ledger digest is invalid'
            USING ERRCODE = '22023';
    END IF;

    BEGIN
        document := convert_from(ledger_bytes, 'UTF8')::jsonb;
        unsigned_document := replace(
            convert_from(ledger_bytes, 'UTF8'),
            format(
                ',"ledger_sha256":%s',
                to_jsonb(expected_ledger_sha256::text)::text
            ),
            ''
        );
        SELECT count(*) INTO key_count
        FROM jsonb_object_keys(document);
        IF jsonb_typeof(document) <> 'object'
           OR key_count <> 5
           OR EXISTS (
               SELECT 1
               FROM jsonb_object_keys(document) AS object_key(key)
               WHERE object_key.key NOT IN (
                   'entries', 'generated_at', 'ledger_sha256', 'profile',
                   'schema_version'
               )
           )
           OR document->>'profile' <> 'palimpsest-deletion-fence-ledger-v1'
           OR (document->>'schema_version')::integer <> 1
           OR document->>'ledger_sha256' <> expected_ledger_sha256
           OR length(convert_from(ledger_bytes, 'UTF8'))
                  - length(unsigned_document)
                  <> length(
                      format(
                          ',"ledger_sha256":%s',
                          to_jsonb(expected_ledger_sha256::text)::text
                      )
                  )
           OR encode(
                  public.digest(
                      convert_to(unsigned_document, 'UTF8'),
                      'sha256'
                  ),
                  'hex'
              ) <> expected_ledger_sha256 THEN
            RAISE EXCEPTION 'restore fence ledger metadata is invalid'
                USING ERRCODE = '22023';
        END IF;

        generated_at := (document->>'generated_at')::timestamptz;
        IF NOT isfinite(generated_at) OR generated_at > clock_timestamp() THEN
            RAISE EXCEPTION 'restore fence ledger timestamp is invalid'
                USING ERRCODE = '22023';
        END IF;
        entries := document->'entries';
        IF jsonb_typeof(entries) <> 'array'
           OR jsonb_array_length(entries) > 100000 THEN
            RAISE EXCEPTION 'restore fence ledger entries are invalid'
                USING ERRCODE = '22023';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM jsonb_array_elements(entries) AS item(value)
            WHERE jsonb_typeof(item.value) <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(item.value)) <> 4
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_object_keys(item.value) AS entry_key(key)
                   WHERE entry_key.key NOT IN (
                       'deletion_watermark', 'expires_at', 'scope_digest',
                       'state_version'
                   )
               )
        ) THEN
            RAISE EXCEPTION 'restore fence ledger entry metadata is invalid'
                USING ERRCODE = '22023';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM (
                SELECT
                    item.value->>'scope_digest' AS scope_digest,
                    lag(item.value->>'scope_digest') OVER (
                        ORDER BY item.ordinality
                    ) AS previous_scope
                FROM jsonb_array_elements(entries)
                    WITH ORDINALITY AS item(value, ordinality)
            ) AS ordered
            WHERE ordered.previous_scope IS NOT NULL
              AND ordered.scope_digest <= ordered.previous_scope
        ) THEN
            RAISE EXCEPTION 'restore fence ledger entries are not ordered'
                USING ERRCODE = '22023';
        END IF;

        DROP TABLE IF EXISTS pg_temp.restore_fence_entries;
        CREATE TEMP TABLE restore_fence_entries (
            scope_digest text PRIMARY KEY CHECK (
                scope_digest ~ '^v[0-9]+:[0-9a-f]{64}$'
            ),
            state_version bigint NOT NULL CHECK (state_version > 0),
            deletion_watermark timestamptz NOT NULL,
            expires_at timestamptz NOT NULL
        ) ON COMMIT DROP;
        INSERT INTO restore_fence_entries (
            scope_digest,
            state_version,
            deletion_watermark,
            expires_at
        )
        SELECT entry.scope_digest,
               entry.state_version,
               entry.deletion_watermark::timestamptz,
               entry.expires_at::timestamptz
        FROM jsonb_to_recordset(entries) AS entry(
            scope_digest text,
            state_version bigint,
            deletion_watermark text,
            expires_at text
        );
        IF EXISTS (
            SELECT 1
            FROM restore_fence_entries
            WHERE NOT isfinite(deletion_watermark)
               OR NOT isfinite(expires_at)
               OR deletion_watermark > clock_timestamp()
               OR expires_at <= clock_timestamp()
               OR expires_at <= deletion_watermark
        ) THEN
            RAISE EXCEPTION 'restore fence ledger entry timestamps are invalid'
                USING ERRCODE = '22023';
        END IF;
    EXCEPTION
        WHEN others THEN
            IF SQLSTATE = '22023' THEN
                RAISE;
            END IF;
            RAISE EXCEPTION 'restore fence ledger metadata is invalid'
                USING ERRCODE = '22023';
    END;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('palimpsest.restore:' || expected_ledger_sha256, 0)
    );
    SELECT receipt.*
    INTO existing_receipt
    FROM memory.restore_replay_receipts AS receipt
    WHERE rtrim(receipt.ledger_sha256::text) = expected_ledger_sha256;
    IF FOUND THEN
        RETURN QUERY SELECT
            existing_receipt.scopes_purged,
            existing_receipt.scopes_purged,
            existing_receipt.residual_rows,
            expected_ledger_sha256;
        RETURN;
    END IF;

    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    DROP TABLE IF EXISTS pg_temp.restore_fence_scopes;
    CREATE TEMP TABLE restore_fence_scopes (
        tenant_id uuid NOT NULL,
        subject_id uuid NOT NULL,
        scope_digest text PRIMARY KEY
    ) ON COMMIT DROP;
    INSERT INTO restore_fence_scopes (tenant_id, subject_id, scope_digest)
    SELECT lifecycle.tenant_id,
           lifecycle.subject_id,
           entry.scope_digest
    FROM memory.subject_lifecycles AS lifecycle
    JOIN restore_fence_entries AS entry
      ON entry.scope_digest = memory.deletion_scope_digest(
             lifecycle.tenant_id,
             lifecycle.subject_id
         );
    GET DIAGNOSTICS scopes_found = ROW_COUNT;
    SELECT count(*) INTO entry_count FROM restore_fence_entries;
    IF scopes_found <> entry_count THEN
        RAISE EXCEPTION 'restore fence ledger did not match every subject scope'
            USING ERRCODE = 'P0001';
    END IF;

    replay_session_id := gen_random_uuid();
    INSERT INTO memory.restore_replay_sessions (
        session_id, ledger_sha256, expires_at
    )
    VALUES (
        replay_session_id,
        expected_ledger_sha256,
        clock_timestamp() + interval '5 minutes'
    );
    PERFORM set_config(
        'palimpsest.restore_session_id',
        replay_session_id::text,
        true
    );
    PERFORM set_config(
        'palimpsest.restore_ledger_sha256',
        expected_ledger_sha256,
        true
    );

    scopes_purged := 0;
    residual_rows := 0;
    FOR scope IN
        SELECT tenant_id, subject_id
        FROM restore_fence_scopes
        ORDER BY tenant_id, subject_id
    LOOP
        residual_scope := memory.restore_purge_scope(
            scope.tenant_id,
            scope.subject_id
        );
        scopes_purged := scopes_purged + 1;
        residual_rows := residual_rows + residual_scope;
    END LOOP;
    IF residual_rows <> 0 THEN
        RAISE EXCEPTION 'restore fence replay left residual rows'
            USING ERRCODE = 'P0001';
    END IF;

    UPDATE memory.restore_replay_sessions
    SET completed_at = clock_timestamp()
    WHERE restore_replay_sessions.session_id = replay_session_id;
    INSERT INTO memory.restore_replay_receipts (
        ledger_sha256,
        profile,
        schema_version,
        scopes_purged,
        residual_rows
    )
    VALUES (
        expected_ledger_sha256,
        'palimpsest-deletion-fence-ledger-v1',
        1,
        scopes_purged,
        residual_rows
    );
    ledger_sha256 := expected_ledger_sha256;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION memory.replay_restore_fence_ledger(bytea, text)
FROM PUBLIC;
