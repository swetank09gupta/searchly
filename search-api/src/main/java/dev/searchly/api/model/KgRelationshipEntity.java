package dev.searchly.api.model;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;

@Entity
@Table(name = "kg_relationships")
public class KgRelationshipEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "from_type", nullable = false, length = 50)
    private String fromType;

    @Column(name = "from_id", nullable = false, length = 255)
    private String fromId;

    @Column(name = "relation", nullable = false, length = 50)
    private String relation;

    @Column(name = "to_type", nullable = false, length = 50)
    private String toType;

    @Column(name = "to_id", nullable = false, length = 255)
    private String toId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "properties", nullable = false, columnDefinition = "jsonb")
    private String propertiesJson;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public KgRelationshipEntity() {}

    public KgRelationshipEntity(String fromType, String fromId, String relation,
                                 String toType, String toId, String tenantId,
                                 String propertiesJson) {
        this.fromType       = fromType;
        this.fromId         = fromId;
        this.relation       = relation;
        this.toType         = toType;
        this.toId           = toId;
        this.tenantId       = tenantId;
        this.propertiesJson = propertiesJson;
        this.createdAt      = Instant.now();
    }

    public Long    getId()             { return id; }
    public String  getFromType()       { return fromType; }
    public String  getFromId()         { return fromId; }
    public String  getRelation()       { return relation; }
    public String  getToType()         { return toType; }
    public String  getToId()           { return toId; }
    public String  getTenantId()       { return tenantId; }
    public String  getPropertiesJson() { return propertiesJson; }
    public Instant getCreatedAt()      { return createdAt; }
}
