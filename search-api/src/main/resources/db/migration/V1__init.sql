CREATE TABLE IF NOT EXISTS tenants (
    id           VARCHAR(64) PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    tier         VARCHAR(32) NOT NULL DEFAULT 'STANDARD',
    quota_docs   BIGINT NOT NULL DEFAULT 50000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS documents (
    id           UUID PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL REFERENCES tenants(id),
    title        VARCHAR(512) NOT NULL,
    content      TEXT NOT NULL,
    metadata     JSONB,
    status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_documents_tenant ON documents (tenant_id);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents (status);

-- Seed dev tenants
INSERT INTO tenants (id, name, tier, quota_docs) VALUES
    ('acme',    'Acme Corp',     'STANDARD',   50000),
    ('globex',  'Globex Inc',    'PREMIUM',    1000000),
    ('initech', 'Initech',       'FREE',       1000),
    ('umbrella','Umbrella Corp', 'ENTERPRISE', 9223372036854775807)
ON CONFLICT (id) DO NOTHING;
