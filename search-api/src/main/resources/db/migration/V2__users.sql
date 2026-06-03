CREATE TABLE IF NOT EXISTS users (
    id           VARCHAR(64) PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL REFERENCES tenants(id),
    display_name VARCHAR(255) NOT NULL,
    email        VARCHAR(255) NOT NULL,
    roles        TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, email)
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id);

-- Seed users — one TENANT_ADMIN, one EDITOR, one VIEWER per tenant.
-- These map to the X-User-Id / X-User-Roles headers in dev mode.
-- In production these would be provisioned via the IdP (Keycloak/Okta/Auth0).
INSERT INTO users (id, tenant_id, display_name, email, roles) VALUES
    -- acme (STANDARD)
    ('alice',   'acme',     'Alice Admin',   'alice@acme.test',     'TENANT_ADMIN,EDITOR,VIEWER'),
    ('bob',     'acme',     'Bob Editor',    'bob@acme.test',       'EDITOR,VIEWER'),
    ('carol',   'acme',     'Carol Viewer',  'carol@acme.test',     'VIEWER'),
    -- globex (PREMIUM)
    ('dave',    'globex',   'Dave Admin',    'dave@globex.test',    'TENANT_ADMIN,EDITOR,VIEWER'),
    ('eve',     'globex',   'Eve Editor',    'eve@globex.test',     'EDITOR,VIEWER'),
    ('frank',   'globex',   'Frank Viewer',  'frank@globex.test',   'VIEWER'),
    -- initech (FREE)
    ('grace',   'initech',  'Grace Admin',   'grace@initech.test',  'TENANT_ADMIN,EDITOR,VIEWER'),
    ('heidi',   'initech',  'Heidi Editor',  'heidi@initech.test',  'EDITOR,VIEWER'),
    ('ivan',    'initech',  'Ivan Viewer',   'ivan@initech.test',   'VIEWER'),
    -- umbrella (ENTERPRISE)
    ('judy',    'umbrella', 'Judy Admin',    'judy@umbrella.test',  'TENANT_ADMIN,EDITOR,VIEWER'),
    ('mallory', 'umbrella', 'Mallory Editor','mallory@umbrella.test','EDITOR,VIEWER'),
    ('nia',     'umbrella', 'Nia Viewer',    'nia@umbrella.test',   'VIEWER'),
    -- service account for indexer/integration
    ('svc-indexer', 'acme', 'Indexer Service', 'svc@acme.test', 'SERVICE,EDITOR')
ON CONFLICT (id) DO NOTHING;
