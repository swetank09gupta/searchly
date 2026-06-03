package dev.searchly.api.model;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "users")
public class UserEntity {
    @Id
    private String id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "display_name", nullable = false)
    private String displayName;

    @Column(nullable = false)
    private String email;

    @Column(nullable = false)
    private String roles;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public String getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getDisplayName() { return displayName; }
    public String getEmail() { return email; }
    public String getRoles() { return roles; }
    public Instant getCreatedAt() { return createdAt; }
}
