CREATE POLICY fact_revision_current_active_subject
    ON memory.fact_revision_current AS RESTRICTIVE
    FOR ALL
    USING (memory.subject_lifecycle_allows_content(tenant_id, subject_id))
    WITH CHECK (memory.subject_lifecycle_allows_content(tenant_id, subject_id));

CREATE POLICY fact_revision_current_deletion_worker_cleanup
    ON memory.fact_revision_current
    FOR DELETE
    USING (
        tenant_id = NULLIF(current_setting('palimpsest.tenant_id', true), '')::uuid
        AND subject_id = NULLIF(current_setting('palimpsest.subject_id', true), '')::uuid
        AND memory.deletion_workflow_allows(tenant_id, subject_id)
    );
