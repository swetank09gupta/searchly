package dev.searchly.api.controller;

import dev.searchly.api.model.TenantEntity;
import dev.searchly.api.model.UserEntity;
import dev.searchly.api.repository.TenantRepository;
import dev.searchly.api.repository.UserRepository;
import dev.searchly.common.Tier;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.Valid;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Self-service admin API for creating tenants and users without SQL.
 * Auth: X-Admin-Token header matched against SEARCHLY_ADMIN_TOKEN env var.
 * Default in dev: "dev-admin-token". Production: rotate to a secret + ideally swap for SUPER_ADMIN JWT role.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final TenantRepository tenantRepo;
    private final UserRepository userRepo;
    private final String adminToken;

    @PersistenceContext
    private EntityManager em;

    public AdminController(TenantRepository tenantRepo, UserRepository userRepo,
                           @Value("${searchly.admin-token:dev-admin-token}") String adminToken) {
        this.tenantRepo = tenantRepo;
        this.userRepo = userRepo;
        this.adminToken = adminToken;
    }

    private void requireAdmin(String token) {
        if (!Objects.equals(token, adminToken)) {
            throw new SecurityException("Invalid admin token");
        }
    }

    public record CreateTenantRequest(
            @NotBlank @Pattern(regexp = "[a-z0-9][a-z0-9-]{1,62}") String id,
            @NotBlank String name,
            @NotBlank String tier,
            Long quotaDocs
    ) {}

    public record CreateUserRequest(
            @NotBlank @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9-_]{1,62}") String id,
            @NotBlank String displayName,
            @NotBlank String email,
            @NotBlank String roles
    ) {}

    @PostMapping("/tenants")
    @Transactional
    public ResponseEntity<Map<String, Object>> createTenant(
            @RequestHeader(value = "X-Admin-Token", required = false) String token,
            @Valid @RequestBody CreateTenantRequest req) {
        requireAdmin(token);
        Tier tier = Tier.valueOf(req.tier().toUpperCase());
        long quota = req.quotaDocs() != null ? req.quotaDocs() : (long) tier.dailyIndexLimit;
        em.createNativeQuery(
                "INSERT INTO tenants (id, name, tier, quota_docs, created_at) VALUES (?, ?, ?, ?, NOW())")
                .setParameter(1, req.id())
                .setParameter(2, req.name())
                .setParameter(3, tier.name())
                .setParameter(4, quota)
                .executeUpdate();
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "id", req.id(),
                "name", req.name(),
                "tier", tier.name(),
                "quotaDocs", quota,
                "createdAt", Instant.now().toString()));
    }

    @GetMapping("/tenants")
    public List<Map<String, Object>> listTenants(
            @RequestHeader(value = "X-Admin-Token", required = false) String token) {
        requireAdmin(token);
        return tenantRepo.findAll().stream().map(t -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", t.getId());
            m.put("name", t.getName());
            m.put("tier", t.getTier().name());
            m.put("quotaDocs", t.getQuotaDocs());
            m.put("createdAt", t.getCreatedAt());
            return m;
        }).toList();
    }

    @PostMapping("/tenants/{tenantId}/users")
    @Transactional
    public ResponseEntity<Map<String, Object>> createUser(
            @RequestHeader(value = "X-Admin-Token", required = false) String token,
            @PathVariable String tenantId,
            @Valid @RequestBody CreateUserRequest req) {
        requireAdmin(token);
        if (tenantRepo.findById(tenantId).isEmpty()) {
            throw new java.util.NoSuchElementException("Tenant not found: " + tenantId);
        }
        em.createNativeQuery(
                "INSERT INTO users (id, tenant_id, display_name, email, roles, created_at) " +
                "VALUES (?, ?, ?, ?, ?, NOW())")
                .setParameter(1, req.id())
                .setParameter(2, tenantId)
                .setParameter(3, req.displayName())
                .setParameter(4, req.email())
                .setParameter(5, req.roles())
                .executeUpdate();
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "id", req.id(),
                "tenantId", tenantId,
                "displayName", req.displayName(),
                "email", req.email(),
                "roles", req.roles()));
    }

    @GetMapping("/tenants/{tenantId}/users")
    public List<Map<String, Object>> listUsers(
            @RequestHeader(value = "X-Admin-Token", required = false) String token,
            @PathVariable String tenantId) {
        requireAdmin(token);
        return userRepo.findAll().stream()
                .filter(u -> u.getTenantId().equals(tenantId))
                .map(u -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", u.getId());
                    m.put("tenantId", u.getTenantId());
                    m.put("displayName", u.getDisplayName());
                    m.put("email", u.getEmail());
                    m.put("roles", u.getRoles());
                    return m;
                }).toList();
    }
}
