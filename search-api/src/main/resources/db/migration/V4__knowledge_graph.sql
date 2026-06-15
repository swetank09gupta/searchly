CREATE TABLE IF NOT EXISTS kg_entities (
    id           BIGSERIAL PRIMARY KEY,
    entity_type  VARCHAR(50)  NOT NULL,  -- jira_issue | pull_request | commit | service | deployment | customer | release
    entity_id    VARCHAR(255) NOT NULL,  -- external ID: AES-891, sha-abc, service-name, v4.2
    tenant_id    VARCHAR(64)  NOT NULL,
    name         VARCHAR(500),
    properties   JSONB        NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_kg_entity UNIQUE (entity_type, entity_id, tenant_id)
);

CREATE TABLE IF NOT EXISTS kg_relationships (
    id          BIGSERIAL PRIMARY KEY,
    from_type   VARCHAR(50)  NOT NULL,
    from_id     VARCHAR(255) NOT NULL,
    relation    VARCHAR(50)  NOT NULL,   -- implements | fixes | deployed_by | runs | linked_to | generates_logs
    to_type     VARCHAR(50)  NOT NULL,
    to_id       VARCHAR(255) NOT NULL,
    tenant_id   VARCHAR(64)  NOT NULL,
    properties  JSONB        NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_kg_relation UNIQUE (from_type, from_id, relation, to_type, to_id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_kg_entities_lookup ON kg_entities (entity_type, entity_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_kg_rel_from ON kg_relationships (from_type, from_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_kg_rel_to   ON kg_relationships (to_type,   to_id,   tenant_id);
