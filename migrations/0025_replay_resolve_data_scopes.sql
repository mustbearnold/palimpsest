-- spec 016: a scope fenced after a backup has no lifecycle row in the
-- restored copy. The replay now resolves ledger scopes against every
-- data-bearing subject, not only subject_lifecycles rows. The fence ledger
-- remains the independent authority: unmatched scopes abort the restore.

CREATE OR REPLACE FUNCTION memory.replay_restore_fence_ledger(
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

    -- The ledger digests come from the live store's active scope key. The
    -- restored copy carries the key table as of the backup. A version
    -- mismatch means the key rotated between backup and restore: the digests
    -- cannot be re-derived, so the restore fails closed.
    DECLARE
        active_key_version text;
        ledger_key_version text;
    BEGIN
        SELECT key_version
        INTO active_key_version
        FROM memory.deletion_scope_keys
        WHERE active = true
        ORDER BY created_at DESC
        LIMIT 1;

        SELECT min(split_part(scope_digest, ':', 1))
        INTO ledger_key_version
        FROM restore_fence_entries;

        IF ledger_key_version IS NOT NULL
           AND ledger_key_version <> active_key_version THEN
            RAISE EXCEPTION
                'restore fence ledger key version does not match the restored store'
                USING ERRCODE = 'P0001';
        END IF;
    END;

    PERFORM set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true);
    DROP TABLE IF EXISTS pg_temp.restore_fence_scopes;
    CREATE TEMP TABLE restore_fence_scopes (
        tenant_id uuid NOT NULL,
        subject_id uuid NOT NULL,
        scope_digest text PRIMARY KEY
    ) ON COMMIT DROP;
    INSERT INTO restore_fence_scopes (tenant_id, subject_id, scope_digest)
    SELECT candidate.tenant_id,
           candidate.subject_id,
           entry.scope_digest
    FROM (
        SELECT tenant_id, subject_id FROM memory.subject_lifecycles
        UNION
        SELECT tenant_id, subject_id FROM memory.episodes
        UNION
        SELECT tenant_id, subject_id FROM memory.facts
        UNION
        SELECT tenant_id, subject_id FROM memory.fact_revisions
        UNION
        SELECT tenant_id, subject_id FROM memory.checkpoints
        UNION
        SELECT tenant_id, subject_id FROM memory.outbox_intents
        UNION
        SELECT tenant_id, subject_id FROM memory.write_audit_receipts
        UNION
        SELECT tenant_id, subject_id FROM memory.retrieval_receipts
        UNION
        SELECT tenant_id, subject_id FROM memory.export_operations
        UNION
        SELECT tenant_id, subject_id FROM memory.idempotency_receipts
    ) AS candidate
    JOIN restore_fence_entries AS entry
      ON entry.scope_digest = memory.deletion_scope_digest(
             candidate.tenant_id,
             candidate.subject_id
         );
    GET DIAGNOSTICS scopes_found = ROW_COUNT;
    SELECT count(*) INTO entry_count FROM restore_fence_entries;
    -- A ledger scope with no data rows in the restored copy is vacuous:
    -- the copy predates the fence, so there is nothing to suppress. The
    -- remaining per-scope purge still aborts on any residual rows.
    IF scopes_found > entry_count THEN
        RAISE EXCEPTION 'restore fence ledger resolved too many scopes'
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
