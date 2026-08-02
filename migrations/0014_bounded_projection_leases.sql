-- Projection claims use an explicit, observable lease policy.  The policy is
-- migration-owned so workers cannot silently change reclamation timing.
CREATE TABLE memory.embedding_projection_lease_policies (
    policy_id text PRIMARY KEY CHECK (
        btrim(policy_id) <> '' AND length(policy_id) <= 255
    ),
    lease_seconds integer NOT NULL CHECK (lease_seconds BETWEEN 5 AND 3600),
    renewal_interval_seconds integer NOT NULL CHECK (
        renewal_interval_seconds >= 1
        AND renewal_interval_seconds < lease_seconds
    ),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        CHECK (isfinite(created_at))
);

INSERT INTO memory.embedding_projection_lease_policies (
    policy_id,
    lease_seconds,
    renewal_interval_seconds,
    schema_version
)
VALUES ('embedding-projection-v1', 60, 20, 1);

CREATE FUNCTION memory.reject_embedding_projection_lease_policy_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, memory
AS $$
BEGIN
    RAISE EXCEPTION 'embedding projection lease policies are immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER embedding_projection_lease_policies_reject_mutation
BEFORE UPDATE OR DELETE ON memory.embedding_projection_lease_policies
FOR EACH ROW
EXECUTE FUNCTION memory.reject_embedding_projection_lease_policy_mutation();

ALTER TABLE memory.embedding_projection_lease_policies
    ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory.embedding_projection_lease_policies
    FORCE ROW LEVEL SECURITY;
CREATE POLICY embedding_projection_lease_policies_select
ON memory.embedding_projection_lease_policies
FOR SELECT
USING (true);

ALTER TABLE memory.fact_revision_embedding_projections
    ADD COLUMN generation_lease_expires_at timestamptz
        CHECK (
            generation_lease_expires_at IS NULL
            OR isfinite(generation_lease_expires_at)
        );

-- Give claims created by the previous worker version one bounded grace
-- period before the new expiry-based reclaimer can recover them.
UPDATE memory.fact_revision_embedding_projections
SET generation_lease_expires_at = clock_timestamp()
    + make_interval(secs => (
        SELECT lease_seconds
        FROM memory.embedding_projection_lease_policies
        WHERE policy_id = 'embedding-projection-v1'
    ))
WHERE status = 'generating';

ALTER TABLE memory.fact_revision_embedding_projections
    ADD CONSTRAINT fact_revision_embedding_projections_lease_state
    CHECK (
        (status = 'generating') = (generation_lease_expires_at IS NOT NULL)
    );

CREATE INDEX fact_revision_embedding_projections_lease_idx
    ON memory.fact_revision_embedding_projections (
        tenant_id,
        subject_id,
        status,
        generation_lease_expires_at,
        queued_at,
        revision_id
    );
