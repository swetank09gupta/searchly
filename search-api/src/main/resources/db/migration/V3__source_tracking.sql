-- Source document tracking for tombstone / stale deletion support (P0.3).
--
-- Each row tracks one source document (Jira issue, Confluence page, GitHub file)
-- that has been indexed.  After each sync cycle the connector calls the purge
-- endpoint to delete rows (and their OpenSearch/Postgres/MinIO counterparts)
-- whose last_seen_at is older than the sync start time.

CREATE TABLE IF NOT EXISTS source_documents (
    id           BIGSERIAL     PRIMARY KEY,
    source_id    VARCHAR(512)  NOT NULL,
    source_type  VARCHAR(50)   NOT NULL,   -- 'jira' | 'confluence' | 'git'
    tenant_id    VARCHAR(64)   NOT NULL,
    doc_id       VARCHAR(255),             -- UUID assigned by search-api (nullable until indexed)
    last_seen_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_source_documents UNIQUE (source_id, source_type, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_source_docs_tenant_type
    ON source_documents (tenant_id, source_type);

CREATE INDEX IF NOT EXISTS idx_source_docs_last_seen
    ON source_documents (last_seen_at);
