package dev.searchly.api.model;

import dev.searchly.common.Tier;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "tenants")
public class TenantEntity {
    @Id
    private String id;
    private String name;
    @Enumerated(EnumType.STRING)
    private Tier tier;
    @Column(name = "quota_docs")
    private long quotaDocs;
    @Column(name = "created_at")
    private Instant createdAt;

    public String getId() { return id; }
    public String getName() { return name; }
    public Tier getTier() { return tier; }
    public long getQuotaDocs() { return quotaDocs; }
    public Instant getCreatedAt() { return createdAt; }
}
