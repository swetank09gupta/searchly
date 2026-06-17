package dev.searchly.api.model;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;

@Entity
@Table(name = "kg_entities")
public class KgEntityEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "entity_type", nullable = false, length = 50)
    private String entityType;

    @Column(name = "entity_id", nullable = false, length = 255)
    private String entityId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "name", length = 500)
    private String name;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "properties", nullable = false, columnDefinition = "jsonb")
    private String propertiesJson;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public KgEntityEntity() {}

    public KgEntityEntity(String entityType, String entityId, String tenantId,
                           String name, String propertiesJson) {
        this.entityType     = entityType;
        this.entityId       = entityId;
        this.tenantId       = tenantId;
        this.name           = name;
        this.propertiesJson = propertiesJson;
        this.createdAt      = Instant.now();
        this.updatedAt      = Instant.now();
    }

    public Long    getId()             { return id; }
    public String  getEntityType()     { return entityType; }
    public String  getEntityId()       { return entityId; }
    public String  getTenantId()       { return tenantId; }
    public String  getName()           { return name; }
    public String  getPropertiesJson() { return propertiesJson; }
    public Instant getCreatedAt()      { return createdAt; }
    public Instant getUpdatedAt()      { return updatedAt; }

    public void setName(String name)                    { this.name = name; }
    public void setPropertiesJson(String propertiesJson) { this.propertiesJson = propertiesJson; }
    public void setUpdatedAt(Instant updatedAt)         { this.updatedAt = updatedAt; }
}
